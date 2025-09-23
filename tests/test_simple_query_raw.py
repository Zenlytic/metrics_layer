import copy

import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.model.project import Project
from metrics_layer.core.sql.query_errors import ArgumentError

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "testing_snowflake",
    "week_start_day": "sunday",
    "explores": [{"name": "simple_explore", "from": "simple"}],
}
simple_model2 = copy.deepcopy(simple_model)
simple_model2["name"] = "core2"
simple_model_monday = copy.deepcopy(simple_model)
simple_model_monday["week_start_day"] = "monday"

simple_view = {
    "type": "view",
    "name": "simple",
    "model_name": "core",
    "sql_table_name": "analytics.orders",
    "fields": [
        {
            "name": "order_id",
            "field_type": "dimension",
            "type": "string",
            "primary_key": True,
            "sql": "${TABLE}.order_id",
        },
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
        {
            "name": "orders_beginning_of_month",
            "field_type": "measure",
            "type": "count",
            "sql": "${order_id}",
            "filters": None,
            "non_additive_dimension": {
                "name": "order_raw",
                "window_choice": "min",
            },
        },
    ],
}
simple_view2 = copy.deepcopy(simple_view)
simple_view2["name"] = "simple2"
simple_view2["model_name"] = "core2"
# Assign new field names to allow fields to be resolved without the view name
for field in simple_view2["fields"]:
    field["name"] = f"{field['name']}2"


@pytest.fixture(
    params=[
        pytest.param(([simple_model], [simple_view]), id="one_model"),
        pytest.param(([simple_model, simple_model2], [simple_view, simple_view2]), id="two_models"),
    ]
)
def test_models_and_views(request):
    return request.param


@pytest.mark.query
def test_simple_query(connections, test_models_and_views):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["order_id", "channel"])

    correct = (
        "SELECT simple.order_id as simple_order_id,simple.sales_channel as simple_channel,simple.revenue "
    )
    correct += "as simple_total_revenue FROM analytics.orders simple;"
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_simple_query_monday_week_start_date(connections, query_type):
    project = Project(models=[simple_model_monday], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["order_week"], query_type=query_type)

    if query_type == Definitions.bigquery:
        correct = (
            "SELECT CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), ISOWEEK) AS TIMESTAMP) as"
            " simple_order_week,SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
            " GROUP BY simple_order_week;"
        )
    else:
        correct = (
            "SELECT DATE_TRUNC('WEEK', CAST(simple.order_date AS DATE)) as"
            " simple_order_week,SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
            " GROUP BY DATE_TRUNC('WEEK', CAST(simple.order_date AS DATE)) ORDER BY simple_total_revenue DESC"
            " NULLS LAST;"
        )
    assert query == correct


@pytest.mark.query
def test_query_complex_metric(connections, test_models_and_views):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["revenue_per_aov"], dimensions=["order_id", "channel"])

    correct = (
        "SELECT simple.order_id as simple_order_id,simple.sales_channel as simple_channel,simple.revenue as "
    )
    correct += (
        "simple_average_order_value,simple.revenue as simple_total_revenue FROM analytics.orders simple;"
    )
    assert query == correct


@pytest.mark.query
def test_query_complex_metric_with_none_filters_and_non_additive_dimension(
    connections, test_models_and_views
):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["orders_beginning_of_month"], dimensions=["order_id"])

    correct = (
        "WITH cte_orders_beginning_of_month_order_raw AS (SELECT simple.order_id as"
        " simple_order_id,MIN(simple.order_date) as simple_min_order_raw FROM analytics.orders simple GROUP"
        " BY simple.order_id ORDER BY simple_min_order_raw DESC NULLS LAST) SELECT simple.order_id as"
        " simple_order_id,case when"
        " simple.order_date=cte_orders_beginning_of_month_order_raw.simple_min_order_raw then simple.order_id"
        " end as simple_orders_beginning_of_month FROM analytics.orders simple LEFT JOIN"
        " cte_orders_beginning_of_month_order_raw ON"
        " simple.order_id=cte_orders_beginning_of_month_order_raw.simple_order_id;"
    )
    assert query == correct


@pytest.mark.query
def test_query_complex_metric_having_error(connections, test_models_and_views):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    with pytest.raises(ArgumentError) as exc_info:
        conn.get_sql_query(
            metrics=["revenue_per_aov"],
            dimensions=["order_id", "channel"],
            having=[{"field": "revenue_per_aov", "expression": "greater_than", "value": 13}],
        )

    assert exc_info.value


@pytest.mark.query
def test_query_complex_metric_order_by_error(connections, test_models_and_views):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    with pytest.raises(ArgumentError) as exc_info:
        conn.get_sql_query(
            metrics=["revenue_per_aov"],
            dimensions=["order_id", "channel"],
            order_by=[{"field": "revenue_per_aov", "sort": "desc"}],
        )

    assert exc_info.value


@pytest.mark.query
def test_query_complex_metric_all(connections, test_models_and_views):
    models, views = test_models_and_views
    project = Project(models=models, views=views)
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["revenue_per_aov"],
        dimensions=["order_id", "channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
    )

    correct = (
        "SELECT simple.order_id as simple_order_id,simple.sales_channel as simple_channel,simple.revenue as "
    )
    correct += (
        "simple_average_order_value,simple.revenue as simple_total_revenue FROM analytics.orders simple"
    )
    correct += " WHERE simple.sales_channel<>'Email';"
    assert query == correct
