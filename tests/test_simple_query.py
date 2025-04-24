from copy import deepcopy
from datetime import datetime

import pendulum
import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)
from metrics_layer.core.model import Definitions, Project
from metrics_layer.core.parse.connections import BaseConnection
from metrics_layer.core.sql.query_errors import ParseError

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "testing_snowflake",
    "fiscal_month_offset": 1,
    "week_start_day": "sunday",
    "explores": [{"name": "simple_explore", "from": "simple"}],
}

simple_view = {
    "type": "view",
    "name": "simple",
    "model_name": "core",
    "sql_table_name": "analytics.orders",
    "fields": [
        {"field_type": "measure", "type": "count", "name": "count"},
        {
            "field_type": "measure",
            "type": "number",
            "sql": (  # noqa
                "CASE WHEN ${average_order_value} = 0 THEN 0 ELSE ${total_revenue} /"
                " ${average_order_value} END"
            ),
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
            "name": "organic_channels",
            "field_type": "dimension",
            "type": "string",
            "sql": "${TABLE}.sales_channel",
            "filters": [{"field": "channel", "value": "%organic%"}],
        },
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
                "fiscal_month",
                "fiscal_quarter",
                "fiscal_year",
                "fiscal_month_of_year_index",
                "fiscal_month_index",
                "fiscal_quarter_of_year",
                "week_index",
                "week_of_year",
                "week_of_month",
                "month_index",
                "month_of_year",
                "month_of_year_full_name",
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
            "type": "number",
            "sql": "${TABLE}.discount_amt",
            "name": "discount_amt",
        },
        {
            "field_type": "measure",
            "type": "number",
            "sql": "SUM(${TABLE}.revenue) - SUM(${discount_amt}) / nullif(SUM(${TABLE}.revenue), 0)",
            "name": "net_to_gross_pct",
        },
        {
            "field_type": "measure",
            "type": "percentile",
            "percentile": 75,
            "sql": "${TABLE}.revenue",
            "name": "revenue_75th_percentile",
        },
        {
            "field_type": "measure",
            "type": "percentile",
            "percentile": 99,
            "sql": "${discount_amt}",
            "name": "discount_99th_percentile",
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
        "{} simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC NULLS LAST;"
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
        "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize(
    "measure,query_type",
    [
        ("revenue_75th_percentile", Definitions.snowflake),
        ("discount_99th_percentile", Definitions.snowflake),
        ("revenue_75th_percentile", Definitions.databricks),
        ("discount_99th_percentile", Definitions.databricks),
        ("revenue_75th_percentile", Definitions.redshift),
        ("discount_99th_percentile", Definitions.redshift),
        ("revenue_75th_percentile", Definitions.postgres),
        ("discount_99th_percentile", Definitions.postgres),
        ("revenue_75th_percentile", Definitions.duck_db),
        ("discount_99th_percentile", Definitions.duck_db),
        ("revenue_75th_percentile", Definitions.azure_synapse),
        ("discount_99th_percentile", Definitions.azure_synapse),
        ("revenue_75th_percentile", Definitions.sql_server),
        ("discount_99th_percentile", Definitions.sql_server),
        ("revenue_75th_percentile", Definitions.bigquery),
        ("revenue_75th_percentile", Definitions.trino),
        ("revenue_75th_percentile", Definitions.druid),
        ("revenue_75th_percentile", Definitions.mysql),
    ],
)
def test_simple_query_percentile(connections, measure, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)

    if query_type in {
        Definitions.snowflake,
        Definitions.databricks,
        Definitions.redshift,
        Definitions.postgres,
        Definitions.duck_db,
        Definitions.azure_synapse,
        Definitions.sql_server,
    }:
        lookup = {
            "revenue_75th_percentile": "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY simple.revenue)",
            "discount_99th_percentile": "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY simple.discount_amt)",
        }
        if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.duck_db}:
            order_by = f" ORDER BY simple_{measure} DESC NULLS LAST"
        else:
            order_by = ""
        correct = (
            f"SELECT simple.sales_channel as simple_channel,{lookup[measure]} as simple_{measure} FROM"
            f" analytics.orders simple GROUP BY simple.sales_channel{order_by};"
        )
        query = conn.get_sql_query(metrics=[measure], dimensions=["channel"], query_type=query_type)
        assert query == correct
    else:
        with pytest.raises(QueryError) as exc_info:
            conn.get_sql_query(metrics=[measure], dimensions=["channel"], query_type=query_type)

        assert (
            exc_info.value.args[0]
            == f"Percentile is not supported in {query_type}. Please choose another aggregate function for"
            f" the simple.{measure} measure."
        )


@pytest.mark.query
def test_simple_query_measure_number_replacement(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["net_to_gross_pct"], dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) - SUM((simple.discount_amt)) /"
        " nullif(SUM(simple.revenue), 0) as simple_net_to_gross_pct FROM analytics.orders simple GROUP BY"
        " simple.sales_channel ORDER BY simple_net_to_gross_pct DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_dimension_filter(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["organic_channels"])

    correct = (
        "SELECT case when LOWER(simple.sales_channel) LIKE LOWER('%organic%') then simple.sales_channel end"
        " as simple_organic_channels,SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
        " GROUP BY case when LOWER(simple.sales_channel) LIKE LOWER('%organic%') then simple.sales_channel"
        " end ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_field_on_field_filter(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["organic_channels"],
        where=[{"field": "new_vs_repeat", "expression": "greater_than", "value": "simple.group"}],
    )

    correct = (
        "SELECT case when LOWER(simple.sales_channel) LIKE LOWER('%organic%') then simple.sales_channel end"
        " as simple_organic_channels,SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
        " WHERE simple.new_vs_repeat>simple.group_name GROUP BY case when LOWER(simple.sales_channel) LIKE"
        " LOWER('%organic%') then simple.sales_channel end ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
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
        ("max_revenue", Definitions.azure_synapse),
        ("min_revenue", Definitions.azure_synapse),
        ("max_revenue", Definitions.sql_server),
        ("min_revenue", Definitions.sql_server),
        ("max_revenue", Definitions.redshift),
        ("min_revenue", Definitions.redshift),
        ("max_revenue", Definitions.postgres),
        ("min_revenue", Definitions.postgres),
        ("max_revenue", Definitions.trino),
        ("min_revenue", Definitions.trino),
        ("max_revenue", Definitions.bigquery),
        ("min_revenue", Definitions.bigquery),
        ("max_revenue", Definitions.duck_db),
        ("min_revenue", Definitions.duck_db),
        ("max_revenue", Definitions.mysql),
        ("min_revenue", Definitions.mysql),
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
        order_by = f" ORDER BY simple_{agg.lower()}_revenue DESC NULLS LAST"
    else:
        order_by = ""

    if query_type in Definitions.no_semicolon_warehouses:
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
        (Definitions.azure_synapse),
        (Definitions.redshift),
        (Definitions.postgres),
        (Definitions.trino),
        (Definitions.bigquery),
        (Definitions.duck_db),
        (Definitions.mysql),
    ],
)
def test_simple_query_count_distinct(connections, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["unique_groups"], dimensions=["channel"], query_type=query_type)

    group_by = "simple.sales_channel"
    semi = ";"
    if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.duck_db}:
        order_by = f" ORDER BY simple_unique_groups DESC NULLS LAST"
    else:
        order_by = ""

    if query_type in Definitions.no_semicolon_warehouses:
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
        "FROM analytics.orders simple ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_single_dimension(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel FROM analytics.orders simple "
        "GROUP BY simple.sales_channel ORDER BY simple_channel ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type", [Definitions.snowflake, Definitions.sql_server, Definitions.azure_synapse]
)
def test_simple_query_limit(connections, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(dimensions=["channel"], limit=10, query_type=query_type)

    if Definitions.snowflake == query_type:
        correct = (
            "SELECT simple.sales_channel as simple_channel FROM analytics.orders simple "
            "GROUP BY simple.sales_channel ORDER BY simple_channel ASC NULLS LAST LIMIT 10;"
        )
    elif query_type in {Definitions.sql_server, Definitions.azure_synapse}:
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
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_count DESC NULLS LAST;"
    assert query == correct


@pytest.mark.query
def test_simple_query_alias_keyword(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["count"], dimensions=["group"])

    correct = "SELECT simple.group_name as simple_group,COUNT(*) as simple_count FROM "
    correct += "analytics.orders simple GROUP BY simple.group_name ORDER BY simple_count DESC NULLS LAST;"
    assert query == correct


@pytest.mark.parametrize(
    "field,group,query_type",
    [
        ("order", "date", Definitions.snowflake),
        ("order", "week", Definitions.snowflake),
        ("previous_order", "date", Definitions.snowflake),
        ("order", "date", Definitions.trino),
        ("order", "week", Definitions.trino),
        ("previous_order", "date", Definitions.trino),
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
        ("order", "date", Definitions.azure_synapse),
        ("order", "week", Definitions.azure_synapse),
        ("previous_order", "date", Definitions.azure_synapse),
        ("order", "date", Definitions.redshift),
        ("order", "week", Definitions.redshift),
        ("order", "date", Definitions.postgres),
        ("order", "week", Definitions.postgres),
        ("order", "date", Definitions.bigquery),
        ("order", "week", Definitions.bigquery),
        ("order", "date", Definitions.mysql),
        ("order", "week", Definitions.mysql),
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

    semi = ";" if query_type not in Definitions.no_semicolon_warehouses else ""
    date_format = "%Y-%m-%dT%H:%M:%S"
    start = pendulum.now("America/New_York").start_of("month").strftime(date_format)
    if pendulum.now("America/New_York").day == 1:
        end = pendulum.now("America/New_York").end_of("day").strftime(date_format)
    else:
        end = pendulum.now("America/New_York").end_of("day").subtract(days=1).strftime(date_format)

    if query_type == Definitions.snowflake:
        ttype = "TIMESTAMP_NTZ"
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', simple.previous_order_date)"}
        else:
            result_lookup = {
                "date": (  # noqa
                    "DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS"
                    f" {ttype}) AS TIMESTAMP))"
                ),
                "week": (  # noqa
                    "DATE_TRUNC('WEEK', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York',"
                    f" simple.order_date) AS {ttype}) AS TIMESTAMP) AS DATE) + 1) - 1"
                ),
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) "
            f"AS {ttype}) AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS {ttype}) AS TIMESTAMP))<='{end}'"  # noqa
        )
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
    elif query_type == Definitions.redshift:
        ttype = "TIMESTAMP"
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', simple.previous_order_date)"}
        else:
            result_lookup = {
                "date": (  # noqa
                    "DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', CAST(simple.order_date"
                    f" AS TIMESTAMP)) AS {ttype}) AS TIMESTAMP))"
                ),
                "week": (  # noqa
                    "DATE_TRUNC('WEEK', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York',"
                    f" CAST(simple.order_date AS TIMESTAMP)) AS {ttype}) AS TIMESTAMP) AS DATE) + 1) - 1"
                ),
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', CAST(simple.order_date AS"
            " TIMESTAMP)) "
            f"AS {ttype}) AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CONVERT_TIMEZONE('America/New_York', CAST(simple.order_date AS TIMESTAMP)) AS {ttype}) AS TIMESTAMP))<='{end}'"  # noqa
        )
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
    elif query_type == Definitions.databricks:
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
        else:
            result_lookup = {
                "date": (  # noqa
                    f"DATE_TRUNC('DAY', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York',"
                    f" simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))"
                ),
                "week": (  # noqa
                    f"DATE_TRUNC('WEEK', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York',"
                    f" simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP) + INTERVAL '1' DAY) -"
                    f" INTERVAL '1' DAY"
                ),
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) "
            f"AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))<='{end}'"  # noqa
        )
        order_by = ""
    elif query_type == Definitions.mysql:
        if field == "previous_order":
            result_lookup = {"date": "DATE(simple.previous_order_date)"}
        else:
            result_lookup = {
                "date": "DATE(CONVERT_TZ(simple.order_date, 'UTC', 'America/New_York'))",
                "week": (
                    "DATE_SUB(CAST(CONVERT_TZ(simple.order_date, 'UTC', 'America/New_York') AS DATE),"
                    " INTERVAL ((DAYOFWEEK(CAST(CONVERT_TZ(simple.order_date, 'UTC', 'America/New_York') AS"
                    " DATE)) - 1 + 7) % 7) DAY)"
                ),
            }
        where = (
            f"WHERE DATE(CONVERT_TZ(simple.order_date, 'UTC', 'America/New_York'))>='{start}' AND"
            f" DATE(CONVERT_TZ(simple.order_date, 'UTC', 'America/New_York'))<='{end}'"
        )
        order_by = ""
    elif query_type in {Definitions.postgres, Definitions.trino, Definitions.duck_db}:
        if field == "previous_order":
            if query_type in {Definitions.duck_db, Definitions.trino}:
                result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
            else:
                result_lookup = {"date": "DATE_TRUNC('DAY', simple.previous_order_date)"}

        else:
            result_lookup = {
                "date": (  # noqa
                    "DATE_TRUNC('DAY', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at"
                    " time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP))"
                ),
                "week": (  # noqa
                    "DATE_TRUNC('WEEK', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at"
                    " time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL"
                    " '1' DAY"
                ),
            }
        if query_type == Definitions.trino:
            where = (
                "WHERE DATE_TRUNC('DAY', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at time zone 'America/New_York' AS TIMESTAMP) "  # noqa
                f"AS TIMESTAMP))>=CAST('{start}' AS TIMESTAMP) AND DATE_TRUNC('DAY', "
                f"CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP))<=CAST('{end}' AS TIMESTAMP)"  # noqa
            )
        else:
            where = (
                "WHERE DATE_TRUNC('DAY', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at time zone 'America/New_York' AS TIMESTAMP) "  # noqa
                f"AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
                f"CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'UTC' at time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP))<='{end}'"  # noqa
            )
        if query_type == Definitions.duck_db:
            order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
        else:
            order_by = ""
    elif query_type == Definitions.bigquery:
        result_lookup = {
            "date": (  # noqa
                "CAST(DATE_TRUNC(CAST(CAST(DATETIME(CAST(simple.order_date AS TIMESTAMP), 'America/New_York')"
                " AS TIMESTAMP) AS DATE), DAY) AS TIMESTAMP)"
            ),
            "week": (  # noqa
                "CAST(CAST(DATE_TRUNC(CAST(CAST(DATETIME(CAST(simple.order_date AS TIMESTAMP),"
                " 'America/New_York') AS TIMESTAMP) AS DATE) + 1, WEEK) - 1 AS TIMESTAMP) AS TIMESTAMP)"
            ),
        }
        where = f"WHERE simple.order_date>=CAST('{start}' AS TIMESTAMP) AND simple.order_date<=CAST('{end}' AS TIMESTAMP)"  # noqa
        order_by = ""
    elif query_type == Definitions.druid:
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
        else:
            result_lookup = {
                "date": "DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))",  # noqa
                "week": (  # noqa
                    "DATE_TRUNC('WEEK', CAST(simple.order_date AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL"
                    " '1' DAY"
                ),
            }
        where = (
            f"WHERE DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))>='{start}' "
            f"AND DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))<='{end}'"
        )
        order_by = ""
        semi = ""

    elif query_type in {Definitions.sql_server, Definitions.azure_synapse}:
        if field == "previous_order":
            result_lookup = {"date": "CAST(CAST(simple.previous_order_date AS DATE) AS DATETIME)"}
        else:
            result_lookup = {
                "date": "CAST(CAST(simple.order_date AS DATE) AS DATETIME)",  # noqa
                "week": (  # noqa
                    "DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, DATEADD(DAY, 1, CAST(simple.order_date"
                    " AS DATE))), 0))"
                ),
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
        ("fiscal_month", Definitions.snowflake),
        ("fiscal_quarter", Definitions.snowflake),
        ("fiscal_year", Definitions.snowflake),
        ("fiscal_month_of_year_index", Definitions.snowflake),
        ("fiscal_month_index", Definitions.snowflake),
        ("fiscal_quarter_of_year", Definitions.snowflake),
        ("week_index", Definitions.snowflake),
        ("week_of_year", Definitions.snowflake),
        ("week_of_month", Definitions.snowflake),
        ("month_of_year_index", Definitions.snowflake),
        ("month_index", Definitions.snowflake),
        ("month_of_year", Definitions.snowflake),
        ("month_of_year_full_name", Definitions.snowflake),
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
        ("fiscal_month", Definitions.databricks),
        ("fiscal_quarter", Definitions.databricks),
        ("fiscal_year", Definitions.databricks),
        ("fiscal_month_of_year_index", Definitions.databricks),
        ("fiscal_month_index", Definitions.databricks),
        ("fiscal_quarter_of_year", Definitions.databricks),
        ("week_index", Definitions.databricks),
        ("week_of_month", Definitions.databricks),
        ("month_of_year_index", Definitions.databricks),
        ("month_of_year", Definitions.databricks),
        ("month_of_year_full_name", Definitions.databricks),
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
        ("fiscal_month", Definitions.druid),
        ("fiscal_quarter", Definitions.druid),
        ("fiscal_year", Definitions.druid),
        ("fiscal_month_of_year_index", Definitions.druid),
        ("fiscal_month_index", Definitions.druid),
        ("fiscal_quarter_of_year", Definitions.druid),
        ("week_index", Definitions.druid),
        ("week_of_month", Definitions.druid),
        ("month_of_year_index", Definitions.druid),
        ("month_of_year", Definitions.druid),
        ("month_of_year_full_name", Definitions.druid),
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
        ("fiscal_month", Definitions.sql_server),
        ("fiscal_quarter", Definitions.sql_server),
        ("fiscal_year", Definitions.sql_server),
        ("fiscal_month_of_year_index", Definitions.sql_server),
        ("fiscal_month_index", Definitions.sql_server),
        ("fiscal_quarter_of_year", Definitions.sql_server),
        ("week_index", Definitions.sql_server),
        ("week_of_month", Definitions.sql_server),
        ("month_of_year_index", Definitions.sql_server),
        ("month_of_year", Definitions.sql_server),
        ("month_of_year_full_name", Definitions.sql_server),
        ("quarter_of_year", Definitions.sql_server),
        ("hour_of_day", Definitions.sql_server),
        ("day_of_week", Definitions.sql_server),
        ("day_of_month", Definitions.sql_server),
        ("day_of_year", Definitions.sql_server),
        ("time", Definitions.azure_synapse),
        ("second", Definitions.azure_synapse),
        ("minute", Definitions.azure_synapse),
        ("hour", Definitions.azure_synapse),
        ("date", Definitions.azure_synapse),
        ("week", Definitions.azure_synapse),
        ("month", Definitions.azure_synapse),
        ("quarter", Definitions.azure_synapse),
        ("year", Definitions.azure_synapse),
        ("fiscal_month", Definitions.azure_synapse),
        ("fiscal_quarter", Definitions.azure_synapse),
        ("fiscal_year", Definitions.azure_synapse),
        ("fiscal_month_of_year_index", Definitions.azure_synapse),
        ("fiscal_month_index", Definitions.azure_synapse),
        ("fiscal_quarter_of_year", Definitions.azure_synapse),
        ("week_index", Definitions.azure_synapse),
        ("week_of_month", Definitions.azure_synapse),
        ("month_of_year_index", Definitions.azure_synapse),
        ("month_of_year", Definitions.azure_synapse),
        ("month_of_year_full_name", Definitions.azure_synapse),
        ("quarter_of_year", Definitions.azure_synapse),
        ("hour_of_day", Definitions.azure_synapse),
        ("day_of_week", Definitions.azure_synapse),
        ("day_of_month", Definitions.azure_synapse),
        ("day_of_year", Definitions.azure_synapse),
        ("time", Definitions.redshift),
        ("second", Definitions.redshift),
        ("minute", Definitions.redshift),
        ("hour", Definitions.redshift),
        ("date", Definitions.redshift),
        ("week", Definitions.redshift),
        ("month", Definitions.redshift),
        ("quarter", Definitions.redshift),
        ("year", Definitions.redshift),
        ("fiscal_month", Definitions.redshift),
        ("fiscal_quarter", Definitions.redshift),
        ("fiscal_year", Definitions.redshift),
        ("fiscal_month_of_year_index", Definitions.redshift),
        ("fiscal_month_index", Definitions.redshift),
        ("fiscal_quarter_of_year", Definitions.redshift),
        ("week_index", Definitions.redshift),
        ("week_of_month", Definitions.redshift),
        ("month_of_year_index", Definitions.redshift),
        ("month_of_year", Definitions.redshift),
        ("month_of_year_full_name", Definitions.redshift),
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
        ("fiscal_month", Definitions.postgres),
        ("fiscal_quarter", Definitions.postgres),
        ("fiscal_year", Definitions.postgres),
        ("fiscal_month_of_year_index", Definitions.postgres),
        ("fiscal_month_index", Definitions.postgres),
        ("fiscal_quarter_of_year", Definitions.postgres),
        ("week_index", Definitions.postgres),
        ("week_of_month", Definitions.postgres),
        ("month_of_year_index", Definitions.postgres),
        ("month_of_year", Definitions.postgres),
        ("month_of_year_full_name", Definitions.postgres),
        ("quarter_of_year", Definitions.postgres),
        ("hour_of_day", Definitions.postgres),
        ("day_of_week", Definitions.postgres),
        ("day_of_month", Definitions.postgres),
        ("day_of_year", Definitions.postgres),
        ("time", Definitions.trino),
        ("second", Definitions.trino),
        ("minute", Definitions.trino),
        ("hour", Definitions.trino),
        ("date", Definitions.trino),
        ("week", Definitions.trino),
        ("month", Definitions.trino),
        ("quarter", Definitions.trino),
        ("year", Definitions.trino),
        ("fiscal_month", Definitions.trino),
        ("fiscal_quarter", Definitions.trino),
        ("fiscal_year", Definitions.trino),
        ("fiscal_month_of_year_index", Definitions.trino),
        ("fiscal_month_index", Definitions.trino),
        ("fiscal_quarter_of_year", Definitions.trino),
        ("week_index", Definitions.trino),
        ("week_of_month", Definitions.trino),
        ("month_of_year_index", Definitions.trino),
        ("month_of_year", Definitions.trino),
        ("month_of_year_full_name", Definitions.trino),
        ("quarter_of_year", Definitions.trino),
        ("hour_of_day", Definitions.trino),
        ("day_of_week", Definitions.trino),
        ("day_of_month", Definitions.trino),
        ("day_of_year", Definitions.trino),
        ("time", Definitions.duck_db),
        ("second", Definitions.duck_db),
        ("minute", Definitions.duck_db),
        ("hour", Definitions.duck_db),
        ("date", Definitions.duck_db),
        ("week", Definitions.duck_db),
        ("month", Definitions.duck_db),
        ("quarter", Definitions.duck_db),
        ("year", Definitions.duck_db),
        ("fiscal_month", Definitions.duck_db),
        ("fiscal_quarter", Definitions.duck_db),
        ("fiscal_year", Definitions.duck_db),
        ("fiscal_month_of_year_index", Definitions.duck_db),
        ("fiscal_month_index", Definitions.duck_db),
        ("fiscal_quarter_of_year", Definitions.duck_db),
        ("week_index", Definitions.duck_db),
        ("week_of_month", Definitions.duck_db),
        ("month_of_year_index", Definitions.duck_db),
        ("month_of_year", Definitions.duck_db),
        ("month_of_year_full_name", Definitions.duck_db),
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
        ("fiscal_month", Definitions.bigquery),
        ("fiscal_quarter", Definitions.bigquery),
        ("fiscal_year", Definitions.bigquery),
        ("fiscal_month_of_year_index", Definitions.bigquery),
        ("fiscal_month_index", Definitions.bigquery),
        ("fiscal_quarter_of_year", Definitions.bigquery),
        ("week_index", Definitions.bigquery),
        ("week_of_month", Definitions.bigquery),
        ("month_of_year_index", Definitions.bigquery),
        ("month_of_year", Definitions.bigquery),
        ("month_of_year_full_name", Definitions.bigquery),
        ("quarter_of_year", Definitions.bigquery),
        ("hour_of_day", Definitions.bigquery),
        ("day_of_week", Definitions.bigquery),
        ("day_of_month", Definitions.bigquery),
        ("day_of_year", Definitions.bigquery),
        ("time", Definitions.mysql),
        ("second", Definitions.mysql),
        ("minute", Definitions.mysql),
        ("hour", Definitions.mysql),
        ("date", Definitions.mysql),
        ("week", Definitions.mysql),
        ("month", Definitions.mysql),
        ("quarter", Definitions.mysql),
        ("year", Definitions.mysql),
        ("fiscal_month", Definitions.mysql),
        ("fiscal_quarter", Definitions.mysql),
        ("fiscal_year", Definitions.mysql),
        ("fiscal_month_of_year_index", Definitions.mysql),
        ("fiscal_month_index", Definitions.mysql),
        ("fiscal_quarter_of_year", Definitions.mysql),
        ("week_index", Definitions.mysql),
        ("week_of_month", Definitions.mysql),
        ("month_of_year_index", Definitions.mysql),
        ("month_of_year", Definitions.mysql),
        ("month_of_year_full_name", Definitions.mysql),
        ("quarter_of_year", Definitions.mysql),
        ("hour_of_day", Definitions.mysql),
        ("day_of_week", Definitions.mysql),
        ("day_of_month", Definitions.mysql),
        ("day_of_year", Definitions.mysql),
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

    semi = ";" if query_type not in Definitions.no_semicolon_warehouses else ""
    if query_type in {Definitions.snowflake, Definitions.redshift}:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "second": "DATE_TRUNC('SECOND', simple.order_date)",
            "minute": "DATE_TRUNC('MINUTE', simple.order_date)",
            "hour": "DATE_TRUNC('HOUR', simple.order_date)",
            "date": "DATE_TRUNC('DAY', simple.order_date)",
            "week": "DATE_TRUNC('WEEK', CAST(simple.order_date AS DATE) + 1) - 1",
            "month": "DATE_TRUNC('MONTH', simple.order_date)",
            "quarter": (  # noqa
                "CONCAT(EXTRACT(YEAR FROM simple.order_date), '-Q', EXTRACT(QUARTER FROM simple.order_date))"
            ),
            "year": "DATE_TRUNC('YEAR', simple.order_date)",
            "fiscal_month": "DATE_TRUNC('MONTH', DATEADD(MONTH, 1, simple.order_date))",
            "fiscal_quarter": (
                "CONCAT(EXTRACT(YEAR FROM DATEADD(MONTH, 1, simple.order_date)), '-Q', EXTRACT(QUARTER FROM"
                " DATEADD(MONTH, 1, simple.order_date)))"
            ),
            "fiscal_year": "DATE_TRUNC('YEAR', DATEADD(MONTH, 1, simple.order_date))",
            "fiscal_month_of_year_index": f"EXTRACT(MONTH FROM DATEADD(MONTH, 1, simple.order_date))",
            "fiscal_month_index": f"EXTRACT(MONTH FROM DATEADD(MONTH, 1, simple.order_date))",
            "fiscal_quarter_of_year": "EXTRACT(QUARTER FROM DATEADD(MONTH, 1, simple.order_date))",
            "week_index": f"EXTRACT(WEEK FROM DATE_TRUNC('DAY', CAST(simple.order_date AS DATE) + 1))",
            "week_of_year": f"EXTRACT(WEEK FROM DATE_TRUNC('DAY', CAST(simple.order_date AS DATE) + 1))",
            "week_of_month": (
                f"EXTRACT(WEEK FROM simple.order_date) -"
                f" EXTRACT(WEEK FROM DATE_TRUNC('MONTH', simple.order_date)) + 1"
            ),
            "month_of_year_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_of_year": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Mon')",
            "month_name": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Mon')",
            "month_of_year_full_name": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'MMMM')",
            "quarter_of_year": "EXTRACT(QUARTER FROM simple.order_date)",
            "hour_of_day": "EXTRACT(HOUR FROM CAST(simple.order_date AS TIMESTAMP))",
            "day_of_week": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Dy')",
            "day_of_month": "EXTRACT(DAY FROM simple.order_date)",
            "day_of_year": "EXTRACT(DOY FROM simple.order_date)",
        }
        if query_type == Definitions.redshift:
            result_lookup["month_of_year"] = "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Mon')"
            result_lookup["month_name"] = "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Mon')"
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"

    elif query_type == Definitions.mysql:
        result_lookup = {
            "time": "CAST(simple.order_date AS DATETIME)",
            "second": "DATE_FORMAT(simple.order_date, '%Y-%m-%d %H:%i:%s')",
            "minute": "DATE_FORMAT(simple.order_date, '%Y-%m-%d %H:%i:00')",
            "hour": "DATE_FORMAT(simple.order_date, '%Y-%m-%d %H:00:00')",
            "date": "DATE(simple.order_date)",
            "week": (
                "DATE_SUB(CAST(simple.order_date AS DATE), INTERVAL ((DAYOFWEEK(CAST(simple.order_date AS"
                " DATE)) - 1 + 7) % 7) DAY)"
            ),
            "month": "DATE_FORMAT(simple.order_date, '%Y-%m-01')",
            "quarter": "CONCAT(YEAR(simple.order_date), '-Q', QUARTER(simple.order_date))",
            "year": "DATE_FORMAT(simple.order_date, '%Y-01-01')",
            "fiscal_month": (
                "DATE_FORMAT(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH), '%Y-%m-01')"
            ),
            "fiscal_quarter": (
                "CONCAT(YEAR(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH)), '-Q',"
                " QUARTER(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH)))"
            ),
            "fiscal_year": (
                "DATE_FORMAT(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH), '%Y-01-01')"
            ),
            "fiscal_month_of_year_index": (
                "MONTH(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))"
            ),
            "fiscal_month_index": "MONTH(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))",
            "fiscal_quarter_of_year": "QUARTER(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))",
            "week_index": "WEEK(CAST(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 DAY) AS DATETIME))",
            "week_of_month": "WEEK(simple.order_date) - WEEK(DATE_FORMAT(simple.order_date, '%Y-%m-01')) + 1",
            "month_of_year_index": "MONTH(simple.order_date)",
            "month_of_year": "DATE_FORMAT(simple.order_date, '%b')",
            "month_of_year_full_name": "DATE_FORMAT(simple.order_date, '%M')",
            "quarter_of_year": "QUARTER(simple.order_date)",
            "hour_of_day": "HOUR(simple.order_date)",
            "day_of_week": "DATE_FORMAT(simple.order_date, '%a')",
            "day_of_month": "DAY(simple.order_date)",
            "day_of_year": "DAYOFYEAR(simple.order_date)",
        }
        order_by = ""
    elif query_type in {Definitions.sql_server, Definitions.azure_synapse}:
        result_lookup = {
            "time": "CAST(simple.order_date AS DATETIME)",
            "second": "DATEADD(SECOND, DATEDIFF(SECOND, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "minute": "DATEADD(MINUTE, DATEDIFF(MINUTE, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "hour": "DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "date": "CAST(CAST(simple.order_date AS DATE) AS DATETIME)",
            "week": (  # noqa
                "DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, DATEADD(DAY, 1, CAST(simple.order_date AS"
                " DATE))), 0))"
            ),
            "month": "DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST(simple.order_date AS DATE)), 0)",
            "quarter": (
                "CONCAT(YEAR(CAST(simple.order_date AS DATE)), '-Q', DATEPART(QUARTER, CAST(simple.order_date"
                " AS DATE)))"
            ),
            "year": "DATEADD(YEAR, DATEDIFF(YEAR, 0, CAST(simple.order_date AS DATE)), 0)",
            "fiscal_month": (
                "DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE)), 0)"
            ),
            "fiscal_quarter": (
                "CONCAT(YEAR(CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE)), '-Q', DATEPART(QUARTER,"
                " CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE)))"
            ),
            "fiscal_year": (
                "DATEADD(YEAR, DATEDIFF(YEAR, 0, CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE)), 0)"
            ),
            "fiscal_month_of_year_index": (
                f"EXTRACT(MONTH FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE))"
            ),
            "fiscal_month_index": "EXTRACT(MONTH FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE))",
            "fiscal_quarter_of_year": "DATEPART(QUARTER, CAST(DATEADD(MONTH, 1, simple.order_date) AS DATE))",
            "week_index": (
                f"EXTRACT(WEEK FROM CAST(CAST(DATEADD(DAY, 1, CAST(simple.order_date AS DATE)) AS DATETIME)"
                f" AS DATE))"
            ),
            "week_of_month": (
                f"EXTRACT(WEEK FROM CAST(simple.order_date AS DATE)) - EXTRACT(WEEK FROM DATEADD(MONTH,"
                f" DATEDIFF(MONTH, 0, CAST(simple.order_date AS DATE)), 0)) + 1"
            ),
            "month_of_year_index": f"EXTRACT(MONTH FROM CAST(simple.order_date AS DATE))",
            "month_of_year": "LEFT(DATENAME(MONTH, CAST(simple.order_date AS DATE)), 3)",
            "month_of_year_full_name": "DATENAME(MONTH, CAST(simple.order_date AS DATE))",
            "quarter_of_year": "DATEPART(QUARTER, CAST(simple.order_date AS DATE))",
            "hour_of_day": "DATEPART(HOUR, CAST(simple.order_date AS DATETIME))",
            "day_of_week": "LEFT(DATENAME(WEEKDAY, CAST(simple.order_date AS DATE)), 3)",
            "day_of_month": "DATEPART(DAY, CAST(simple.order_date AS DATE))",
            "day_of_year": "DATEPART(Y, CAST(simple.order_date AS DATE))",
        }
        order_by = ""

    elif query_type in {
        Definitions.trino,
        Definitions.postgres,
        Definitions.databricks,
        Definitions.druid,
        Definitions.duck_db,
    }:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "second": "DATE_TRUNC('SECOND', CAST(simple.order_date AS TIMESTAMP))",
            "minute": "DATE_TRUNC('MINUTE', CAST(simple.order_date AS TIMESTAMP))",
            "hour": "DATE_TRUNC('HOUR', CAST(simple.order_date AS TIMESTAMP))",
            "date": "DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))",
            "week": (  # noqa
                "DATE_TRUNC('WEEK', CAST(simple.order_date AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL"
                " '1' DAY"
            ),
            "month": "DATE_TRUNC('MONTH', CAST(simple.order_date AS TIMESTAMP))",
            "quarter": (
                "CONCAT(EXTRACT(YEAR FROM CAST(simple.order_date AS TIMESTAMP)), '-Q', EXTRACT(QUARTER FROM"
                " CAST(simple.order_date AS TIMESTAMP)))"
            ),
            "year": "DATE_TRUNC('YEAR', CAST(simple.order_date AS TIMESTAMP))",
            "fiscal_month": "DATE_TRUNC('MONTH', CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP))",  # noqa  # noqa  # noqa  # noqa
            "fiscal_quarter": (  # noqa
                "CONCAT(EXTRACT(YEAR FROM CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP)), '-Q',"
                " EXTRACT(QUARTER FROM CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP)))"
            ),
            "fiscal_year": "DATE_TRUNC('YEAR', CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP))",  # noqa  # noqa  # noqa  # noqa
            "fiscal_month_of_year_index": (  # noqa
                f"EXTRACT(MONTH FROM CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP))"
            ),
            "fiscal_month_index": (  # noqa
                f"EXTRACT(MONTH FROM CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP))"
            ),
            "fiscal_quarter_of_year": (  # noqa
                "EXTRACT(QUARTER FROM CAST(simple.order_date + INTERVAL '1' MONTH AS TIMESTAMP))"
            ),
            "week_index": (
                f"EXTRACT(WEEK FROM CAST(DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP) + INTERVAL"
                f" '1' DAY) AS TIMESTAMP))"
            ),
            "week_of_month": (  # noqa
                f"EXTRACT(WEEK FROM CAST(simple.order_date AS TIMESTAMP)) - EXTRACT(WEEK FROM"
                f" DATE_TRUNC('MONTH', CAST(simple.order_date AS TIMESTAMP))) + 1"
            ),
            "month_of_year_index": f"EXTRACT(MONTH FROM CAST(simple.order_date AS TIMESTAMP))",
            "month_of_year": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Mon')",
            "month_of_year_full_name": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Month')",
            "quarter_of_year": "EXTRACT(QUARTER FROM CAST(simple.order_date AS TIMESTAMP))",
            "hour_of_day": "EXTRACT('HOUR' FROM CAST(simple.order_date AS TIMESTAMP))",
            "day_of_week": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Dy')",
            "day_of_month": "EXTRACT('DAY' FROM CAST(simple.order_date AS TIMESTAMP))",
            "day_of_year": "EXTRACT('DOY' FROM CAST(simple.order_date AS TIMESTAMP))",
        }
        if query_type == Definitions.trino:
            result_lookup["month_of_year"] = "FORMAT_DATETIME(CAST(simple.order_date AS TIMESTAMP), 'MMM')"
            result_lookup[
                "month_of_year_full_name"
            ] = "FORMAT_DATETIME(CAST(simple.order_date AS TIMESTAMP), 'MMMM')"
            result_lookup["hour_of_day"] = "EXTRACT(HOUR FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_week"] = "FORMAT_DATETIME(CAST(simple.order_date AS TIMESTAMP), 'EEE')"
            result_lookup["day_of_month"] = "EXTRACT(DAY FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_year"] = "EXTRACT(DOY FROM CAST(simple.order_date AS TIMESTAMP))"

        if query_type == Definitions.duck_db:
            order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
        else:
            order_by = ""

        if query_type == Definitions.databricks:
            result_lookup[
                "fiscal_month"
            ] = "DATE_TRUNC('MONTH', CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP))"
            result_lookup["fiscal_quarter"] = (
                "CONCAT(EXTRACT(YEAR FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP)), '-Q',"
                " EXTRACT(QUARTER FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP)))"
            )
            result_lookup[
                "fiscal_year"
            ] = "DATE_TRUNC('YEAR', CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP))"
            result_lookup[
                "fiscal_month_of_year_index"
            ] = f"EXTRACT(MONTH FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP))"
            result_lookup[
                "fiscal_month_index"
            ] = f"EXTRACT(MONTH FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP))"
            result_lookup[
                "fiscal_quarter_of_year"
            ] = "EXTRACT(QUARTER FROM CAST(DATEADD(MONTH, 1, simple.order_date) AS TIMESTAMP))"
            result_lookup["month_of_year"] = "DATE_FORMAT(CAST(simple.order_date AS TIMESTAMP), 'MMM')"
            result_lookup[
                "month_of_year_full_name"
            ] = "DATE_FORMAT(CAST(simple.order_date AS TIMESTAMP), 'MMMM')"  # noqa
            result_lookup["hour_of_day"] = "EXTRACT(HOUR FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_week"] = "DATE_FORMAT(CAST(simple.order_date AS TIMESTAMP), 'E')"
            result_lookup["day_of_month"] = "EXTRACT(DAY FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_year"] = "EXTRACT(DOY FROM CAST(simple.order_date AS TIMESTAMP))"
        if query_type == Definitions.druid:
            result_lookup[
                "month_of_year"
            ] = "CASE EXTRACT(MONTH FROM CAST(simple.order_date AS TIMESTAMP)) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' ELSE 'Invalid Month' END"  # noqa
            result_lookup[
                "month_of_year_full_name"
            ] = "CASE EXTRACT(MONTH FROM CAST(simple.order_date AS TIMESTAMP)) WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' ELSE 'Invalid Month' END"  # noqa
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
            "week": (
                "CAST(CAST(DATE_TRUNC(CAST(simple.order_date AS DATE) + 1, WEEK) - 1 AS TIMESTAMP) AS"
                " TIMESTAMP)"
            ),
            "month": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), MONTH) AS TIMESTAMP)",
            "quarter": "FORMAT_TIMESTAMP('%Y-Q%Q', CAST(simple.order_date AS TIMESTAMP))",
            "year": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), YEAR) AS TIMESTAMP)",
            "fiscal_month": (
                "CAST(DATE_TRUNC(CAST(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH) AS DATE),"
                " MONTH) AS TIMESTAMP)"
            ),
            "fiscal_quarter": (
                "FORMAT_TIMESTAMP('%Y-Q%Q', CAST(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH)"
                " AS TIMESTAMP))"
            ),
            "fiscal_year": (
                "CAST(DATE_TRUNC(CAST(DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH) AS DATE),"
                " YEAR) AS TIMESTAMP)"
            ),
            "fiscal_month_of_year_index": (
                f"EXTRACT(MONTH FROM DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))"
            ),
            "fiscal_month_index": (
                f"EXTRACT(MONTH FROM DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))"
            ),
            "fiscal_quarter_of_year": (
                "EXTRACT(QUARTER FROM DATE_ADD(CAST(simple.order_date AS DATE), INTERVAL 1 MONTH))"
            ),
            "week_index": f"EXTRACT(WEEK FROM DATE_TRUNC(CAST(simple.order_date AS DATE) + 1, DAY))",
            "week_of_month": (
                f"EXTRACT(WEEK FROM simple.order_date) - EXTRACT(WEEK FROM DATE_TRUNC(CAST(simple.order_date"
                f" AS DATE), MONTH)) + 1"
            ),
            "month_of_year_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_of_year": "LEFT(FORMAT_DATETIME('%B', CAST(simple.order_date as DATETIME)), 3)",
            "month_of_year_full_name": "FORMAT_DATETIME('%B', CAST(simple.order_date as DATETIME))",
            "quarter_of_year": "EXTRACT(QUARTER FROM simple.order_date)",
            "hour_of_day": f"CAST(CAST(simple.order_date AS STRING FORMAT 'HH24') AS INT64)",
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
        "simple_total_revenue FROM analytics.orders simple "
        f"GROUP BY {date_result if query_type != Definitions.bigquery else f'simple_order_{group}'}"
        f"{order_by}{semi}"
    )
    print(query)
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
        ("second", Definitions.azure_synapse),
        ("minute", Definitions.azure_synapse),
        ("hour", Definitions.azure_synapse),
        ("day", Definitions.azure_synapse),
        ("week", Definitions.azure_synapse),
        ("month", Definitions.azure_synapse),
        ("quarter", Definitions.azure_synapse),
        ("year", Definitions.azure_synapse),
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
        ("second", Definitions.trino),
        ("minute", Definitions.trino),
        ("hour", Definitions.trino),
        ("day", Definitions.trino),
        ("week", Definitions.trino),
        ("month", Definitions.trino),
        ("quarter", Definitions.trino),
        ("year", Definitions.trino),
        ("second", Definitions.duck_db),
        ("minute", Definitions.duck_db),
        ("hour", Definitions.duck_db),
        ("day", Definitions.duck_db),
        ("week", Definitions.duck_db),
        ("month", Definitions.duck_db),
        ("quarter", Definitions.duck_db),
        ("year", Definitions.duck_db),
        ("second", Definitions.mysql),
        ("minute", Definitions.mysql),
        ("hour", Definitions.mysql),
        ("day", Definitions.mysql),
        ("week", Definitions.mysql),
        ("month", Definitions.mysql),
        ("quarter", Definitions.mysql),
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
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
    elif query_type in {Definitions.trino}:
        result_lookup = {
            "second": "DATE_DIFF('SECOND', simple.view_date, simple.order_date)",
            "minute": "DATE_DIFF('MINUTE', simple.view_date, simple.order_date)",
            "hour": "DATE_DIFF('HOUR', simple.view_date, simple.order_date)",
            "day": "DATE_DIFF('DAY', simple.view_date, simple.order_date)",
            "week": "DATE_DIFF('WEEK', simple.view_date, simple.order_date)",
            "month": "DATE_DIFF('MONTH', simple.view_date, simple.order_date)",
            "quarter": "DATE_DIFF('QUARTER', simple.view_date, simple.order_date)",
            "year": "DATE_DIFF('YEAR', simple.view_date, simple.order_date)",
        }
        order_by = ""
        semi = ""
    elif query_type in {Definitions.druid, Definitions.mysql}:
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
        semi = "" if query_type == Definitions.druid else ";"
    elif query_type in {Definitions.sql_server, Definitions.azure_synapse, Definitions.databricks}:
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
            "second": (  # noqa
                "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR',"
                " AGE(simple.order_date, simple.view_date)) * 60 + DATE_PART('MINUTE', AGE(simple.order_date,"
                " simple.view_date)) * 60 + DATE_PART('SECOND', AGE(simple.order_date, simple.view_date))"
            ),
            "minute": (  # noqa
                "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR',"
                " AGE(simple.order_date, simple.view_date)) * 60 + DATE_PART('MINUTE', AGE(simple.order_date,"
                " simple.view_date))"
            ),
            "hour": (  # noqa
                "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR',"
                " AGE(simple.order_date, simple.view_date))"
            ),
            "day": "DATE_PART('DAY', AGE(simple.order_date, simple.view_date))",
            "week": "TRUNC(DATE_PART('DAY', AGE(simple.order_date, simple.view_date))/7)",
            "month": (  # noqa
                "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date)) * 12 + (DATE_PART('month',"
                " AGE(simple.order_date, simple.view_date)))"
            ),
            "quarter": (  # noqa
                "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date)) * 4 + TRUNC(DATE_PART('month',"
                " AGE(simple.order_date, simple.view_date))/3)"
            ),
            "year": "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date))",
        }
        order_by = ""
    else:
        result_lookup = {
            "second": (  # noqa
                "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP),"
                " SECOND)"
            ),
            "minute": (  # noqa
                "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP),"
                " MINUTE)"
            ),
            "hour": (  # noqa
                "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP),"
                " HOUR)"
            ),
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
        "GROUP BY simple.sales_channel,simple.new_vs_repeat ORDER BY simple_total_revenue DESC NULLS LAST;"
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
        "ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_custom_dimension(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["is_valid_order"])

    correct = (
        "SELECT (CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END) as "
        "simple_is_valid_order,SUM(simple.revenue) as simple_total_revenue "
        "FROM analytics.orders simple GROUP BY (CASE WHEN simple.sales_channel "
        "!= 'fraud' THEN TRUE ELSE FALSE END) ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_custom_metric(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["revenue_per_aov"], dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,CASE WHEN (AVG(simple.revenue)) = 0 THEN 0 ELSE"
        " (SUM(simple.revenue)) / (AVG(simple.revenue)) END as simple_revenue_per_aov FROM analytics.orders"
        " simple GROUP BY simple.sales_channel ORDER BY simple_revenue_per_aov DESC NULLS LAST;"
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
        ("order_date", "greater_than", "2021-08-04", Definitions.trino),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.databricks),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.druid),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.sql_server),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.duck_db),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.trino),
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
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.trino),
        ("order_date", "matches", "last week", Definitions.snowflake),
        ("order_date", "matches", "last year", Definitions.snowflake),
        ("order_date", "matches", "last year", Definitions.databricks),
        ("order_date", "matches", "last year", Definitions.druid),
        ("order_date", "matches", "last year", Definitions.sql_server),
        ("order_date", "matches", "last year", Definitions.redshift),
        ("order_date", "matches", "last year", Definitions.bigquery),
        ("order_date", "matches", "last year", Definitions.duck_db),
        ("order_date", "matches", "last year", Definitions.trino),
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
        Definitions.trino,
    }:
        order_by = ""
    else:
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"

    semi = ";"
    if query_type in {Definitions.druid, Definitions.trino}:
        semi = ""
    sf_or_rs = query_type in {
        Definitions.snowflake,
        Definitions.redshift,
        Definitions.druid,
        Definitions.duck_db,
        Definitions.databricks,
        Definitions.trino,
    }
    if query_type not in {
        Definitions.druid,
        Definitions.duck_db,
        Definitions.databricks,
        Definitions.trino,
    }:
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
    elif sf_or_rs and query_type != Definitions.trino and isinstance(value, datetime):
        condition = f"DATE_TRUNC('DAY', {field_id})>'2021-08-04T00:00:00'"
    elif (
        query_type == Definitions.bigquery
        and expression == "greater_than"
        and isinstance(value, str)
        and field == "order_date"
    ):
        condition = "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)>'2021-08-04'"
    elif query_type == Definitions.bigquery and isinstance(value, datetime) and field == "order_date":
        condition = "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)>CAST('2021-08-04 00:00:00' AS TIMESTAMP)"  # noqa
    elif query_type == Definitions.trino and isinstance(value, datetime) and field == "order_date":
        condition = "DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))>CAST('2021-08-04 00:00:00' AS TIMESTAMP)"  # noqa
    elif (
        query_type == Definitions.bigquery and isinstance(value, datetime) and field == "previous_order_date"
    ):
        condition = "CAST(DATE_TRUNC(CAST(simple.previous_order_date AS DATE), DAY) AS DATETIME)>CAST(CAST('2021-08-04 00:00:00' AS TIMESTAMP) AS DATETIME)"  # noqa
    elif query_type == Definitions.trino and isinstance(value, datetime) and field == "first_order_date":
        condition = "DATE_TRUNC('DAY', CAST(simple.first_order_date AS TIMESTAMP))>CAST(CAST('2021-08-04 00:00:00' AS TIMESTAMP) AS DATE)"  # noqa
    elif query_type == Definitions.bigquery and isinstance(value, datetime) and field == "first_order_date":
        condition = "CAST(DATE_TRUNC(CAST(simple.first_order_date AS DATE), DAY) AS DATE)>CAST(CAST('2021-08-04 00:00:00' AS TIMESTAMP) AS DATE)"  # noqa
    elif sf_or_rs and expression == "matches" and value == "last year":
        last_year = pendulum.now("UTC").year - 1
        if query_type == Definitions.trino:
            start_of = f"CAST('{last_year}-01-01T00:00:00' AS TIMESTAMP)"
            end_of = f"CAST('{last_year}-12-31T23:59:59' AS TIMESTAMP)"
        else:
            start_of = f"'{last_year}-01-01T00:00:00'"
            end_of = f"'{last_year}-12-31T23:59:59'"
        condition = f"DATE_TRUNC('DAY', {field_id})>={start_of} AND "
        condition += f"DATE_TRUNC('DAY', {field_id})<={end_of}"
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
        if query_type == Definitions.trino:
            start_of = f"CAST('{start_of}' AS TIMESTAMP)"
            end_of = f"CAST('{end_of}' AS TIMESTAMP)"
        else:
            start_of = f"'{start_of}'"
            end_of = f"'{end_of}'"
        condition = f"DATE_TRUNC('DAY', {field_id})>={start_of} AND "
        condition += f"DATE_TRUNC('DAY', {field_id})<={end_of}"
        pendulum.week_starts_at(pendulum.MONDAY)
        pendulum.week_ends_at(pendulum.SUNDAY)
    elif query_type == Definitions.bigquery and expression == "matches":
        last_year = pendulum.now("UTC").year - 1
        condition = f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)>=CAST('{last_year}-01-01T00:00:00' AS TIMESTAMP) AND "  # noqa
        condition += f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)<=CAST('{last_year}-12-31T23:59:59' AS TIMESTAMP)"  # noqa

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
        "GROUP BY DATE_TRUNC('DAY', simple.order_date) ORDER BY simple_total_revenue DESC NULLS LAST;"
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
        ("channel", "contains_case_insensitive", "Email", Definitions.trino),
        ("channel", "contains_case_insensitive", "Email", Definitions.mysql),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.databricks),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.druid),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.sql_server),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.trino),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.mysql),
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
        ("is_valid_order", "is_not_null", None, Definitions.trino),
        ("is_valid_order", "is_not_null", None, Definitions.mysql),
        ("is_valid_order", "boolean_true", None, Definitions.snowflake),
        ("is_valid_order", "boolean_false", None, Definitions.snowflake),
        ("is_valid_order", "boolean_true", None, Definitions.sql_server),
        ("is_valid_order", "boolean_false", None, Definitions.sql_server),
        ("is_valid_order", "boolean_true", None, Definitions.trino),
        ("is_valid_order", "boolean_false", None, Definitions.trino),
        ("is_valid_order", "boolean_true", None, Definitions.mysql),
        ("is_valid_order", "boolean_false", None, Definitions.mysql),
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
        order_by = " ORDER BY simple_total_revenue DESC NULLS LAST"
        semi = ";"
    elif query_type in {Definitions.druid, Definitions.trino}:
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
        "is_valid_order": "(CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END)",
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
        "ORDER BY simple_total_revenue DESC NULLS LAST;"
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
        "ORDER BY simple_total_revenue DESC NULLS LAST;"
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
        "ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type",
    [
        Definitions.snowflake,
        Definitions.bigquery,
        Definitions.redshift,
        Definitions.postgres,
        Definitions.trino,
        Definitions.druid,
        Definitions.sql_server,
        Definitions.duck_db,
        Definitions.databricks,
        Definitions.azure_synapse,
        Definitions.mysql,
    ],
)
def test_simple_query_with_order_by_dict(connections, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue", "average_order_value", "max_revenue"],
        dimensions=["channel"],
        order_by=[
            {"field": "total_revenue", "sort": "asc"},
            {"field": "average_order_value"},
            {"field": "max_revenue", "sort": "desc"},
        ],
        query_type=query_type,
    )

    if query_type == Definitions.bigquery:
        group_by = "simple_channel"
    else:
        group_by = "simple.sales_channel"

    semi = ";" if query_type not in {Definitions.druid, Definitions.trino} else ""
    if query_type in {
        Definitions.snowflake,
        Definitions.redshift,
        Definitions.duck_db,
        Definitions.postgres,
        Definitions.trino,
        Definitions.databricks,
        Definitions.bigquery,
    }:
        nulls_last = " NULLS LAST"
    else:
        nulls_last = ""
    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as"
        " simple_total_revenue,AVG(simple.revenue) as simple_average_order_value,MAX(simple.revenue) as"
        f" simple_max_revenue FROM analytics.orders simple GROUP BY {group_by} ORDER BY"
        f" simple_total_revenue ASC{nulls_last},simple_average_order_value ASC{nulls_last},simple_max_revenue"
        f" DESC{nulls_last}{semi}"
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
        "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue ASC NULLS LAST;"
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
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM"
        " analytics.orders simple WHERE simple.sales_channel<>'Email' GROUP BY simple.sales_channel HAVING"
        " SUM(simple.revenue)>12 ORDER BY simple_total_revenue ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_or_filters_no_nesting(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[
            {
                "logical_operator": "OR",
                "conditions": [
                    {"field": "channel", "expression": "not_equal_to", "value": "Email"},
                    {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
                ],
            },
            {"field": "discount_amt", "expression": "greater_than", "value": 1335},
        ],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM"
        " analytics.orders simple WHERE (simple.sales_channel<>'Email' OR simple.new_vs_repeat='New') AND"
        " simple.discount_amt>1335 GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC NULLS"
        " LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_or_filters_single_nesting(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[
            {
                "logical_operator": "OR",
                "conditions": [
                    {
                        "logical_operator": "AND",
                        "conditions": [
                            {"field": "channel", "expression": "not_equal_to", "value": "Email"},
                            {"field": "discount_amt", "expression": "less_than", "value": 0.01},
                        ],
                    },
                    {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
                ],
            },
            {"field": "discount_amt", "expression": "greater_than", "value": 1335},
        ],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM"
        " analytics.orders simple WHERE ((simple.sales_channel<>'Email' AND simple.discount_amt<0.01) OR"
        " simple.new_vs_repeat='New') AND simple.discount_amt>1335 GROUP BY simple.sales_channel ORDER BY"
        " simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_or_filters_triple_nesting(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[
            {"field": "discount_amt", "expression": "greater_than", "value": 1335},
            {
                "logical_operator": "OR",
                "conditions": [
                    {
                        "logical_operator": "AND",
                        "conditions": [
                            {"field": "channel", "expression": "not_equal_to", "value": "Email"},
                            {"field": "discount_amt", "expression": "less_than", "value": 0.01},
                            {
                                "logical_operator": "OR",
                                "conditions": [
                                    {"field": "channel", "expression": "equal_to", "value": "Email"},
                                    {"field": "discount_amt", "expression": "less_than", "value": -100.05},
                                    {
                                        "logical_operator": "AND",
                                        "conditions": [
                                            {
                                                "field": "channel",
                                                "expression": "equal_to",
                                                "value": "Facebook",
                                            },
                                            {
                                                "field": "new_vs_repeat",
                                                "expression": "equal_to",
                                                "value": "Repeat",
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                    {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
                ],
            },
            {
                "logical_operator": "OR",
                "conditions": [
                    {"field": "channel", "expression": "not_equal_to", "value": "Email"},
                    {"field": "discount_amt", "expression": "less_than", "value": 0.01},
                ],
            },
            {"field": "discount_amt", "expression": "greater_than", "value": 13},
        ],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM"
        " analytics.orders simple WHERE simple.discount_amt>1335 AND ((simple.sales_channel<>'Email' AND"
        " simple.discount_amt<0.01 AND (simple.sales_channel='Email' OR simple.discount_amt<-100.05 OR"
        " (simple.sales_channel='Facebook' AND simple.new_vs_repeat='Repeat'))) OR"
        " simple.new_vs_repeat='New') AND (simple.sales_channel<>'Email' OR simple.discount_amt<0.01) AND"
        " simple.discount_amt>13 GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_or_filters_having(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[
            {
                "logical_operator": "OR",
                "conditions": [
                    {"field": "average_order_value", "expression": "greater_than", "value": 250},
                    {"field": "total_revenue", "expression": "less_than", "value": 25000},
                ],
            },
            {"field": "total_revenue", "expression": "greater_than", "value": 20000},
        ],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM"
        " analytics.orders simple GROUP BY simple.sales_channel HAVING (AVG(simple.revenue)>250 OR"
        " SUM(simple.revenue)<25000) AND SUM(simple.revenue)>20000 ORDER BY simple_total_revenue DESC NULLS"
        " LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_or_filters_errors(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    with pytest.raises(ParseError) as exc_info:
        conn.get_sql_query(
            metrics=["total_revenue"],
            dimensions=["channel"],
            where=[
                {
                    "logical_operator": "ORR",
                    "conditions": [
                        {"field": "channel", "expression": "not_equal_to", "value": "Email"},
                        {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
                    ],
                }
            ],
        )

    assert exc_info.value
    assert "needs a valid logical operator. Options are: ['AND', 'OR']" in str(exc_info.value)


@pytest.mark.query
@pytest.mark.parametrize("filter_type", ["where", "having"])
def test_simple_query_with_or_filters_invalid_field_types(connections, filter_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    logical_filter = [
        {
            "logical_operator": "OR",
            "conditions": [
                {"field": "average_order_value", "expression": "greater_than", "value": 250},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
            ],
        },
    ]

    if filter_type == "where":
        filter_dict = {"where": logical_filter}
    else:
        filter_dict = {"having": logical_filter}
    with pytest.raises(QueryError) as exc_info:
        conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"], **filter_dict)

    assert exc_info.value
    assert "Cannot mix dimensions and measures in a compound filter with a logical_operator" in str(
        exc_info.value
    )
