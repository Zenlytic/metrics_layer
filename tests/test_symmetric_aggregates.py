import datetime

import pytest

from metrics_layer.core.model.definitions import Definitions


@pytest.mark.parametrize(
    "query_type",
    [
        Definitions.snowflake,
        Definitions.bigquery,
        Definitions.redshift,
        Definitions.postgres,
        Definitions.azure_synapse,
        Definitions.sql_server,
    ],
)
@pytest.mark.query
def test_query_sum_with_sql(connection, query_type):
    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["channel"], query_type=query_type)

    if query_type in {Definitions.snowflake}:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_total_revenue DESC NULLS LAST"
    elif query_type == Definitions.redshift:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (FARMFINGERPRINT64(orders.id)"
            ")::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (FARMFINGERPRINT64(orders.id))"
            "::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_total_revenue DESC NULLS LAST"
    elif query_type in {Definitions.postgres}:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) * (1000000 * 1.0)) AS"
            " DECIMAL(38,0))) + (HASHTEXTEXTENDED(CAST(orders.id AS TEXT), 0))::NUMERIC(38, 0)) -"
            " SUM(DISTINCT (HASHTEXTEXTENDED(CAST(orders.id AS TEXT), 0))::NUMERIC(38, 0))) AS DOUBLE"
            " PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = ""
    elif query_type == Definitions.bigquery:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
        order_by = ""
    elif query_type in {Definitions.azure_synapse, Definitions.sql_server}:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) * (1000000 * 1.0)) AS"
            " DECIMAL(38,0))) + ABS(CAST(HASHBYTES('MD5', CAST(orders.id AS NVARCHAR(MAX))) AS BIGINT)) %"
            " 10000000000000000000000000) - SUM(DISTINCT ABS(CAST(HASHBYTES('MD5', CAST(orders.id AS"
            " NVARCHAR(MAX))) AS BIGINT)) % 10000000000000000000000000)) AS FLOAT) / CAST((1000000*1.0) AS"
            " FLOAT), 0)"
        )
        order_by = ""
    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        f"{sa} as orders_total_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        f"GROUP BY {'order_lines.sales_channel' if query_type != Definitions.bigquery else 'order_lines_channel'}"  # noqa
        f"{order_by};"
    )
    assert query == correct


@pytest.mark.query
def test_query_count_with_sql(connection):
    query = connection.get_sql_query(metrics=["number_of_orders"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0)"
        " as orders_number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel ORDER BY orders_number_of_orders DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_count_with_one_to_many(connection):
    query = connection.get_sql_query(
        metrics=["number_of_email_purchased_items"], dimensions=["discount_code"]
    )

    correct = (
        "SELECT discounts.code as discounts_discount_code,NULLIF(COUNT(DISTINCT "
        "CASE WHEN  (case when order_lines.sales_channel='Email' then "
        "order_lines.order_id end)  IS NOT NULL THEN  case when order_lines.sales_channel='Email'"
        " then order_lines.order_line_id end  ELSE NULL END), 0) as order_lines_number_of_email_purchased_items "  # noqa
        "FROM analytics_live.discounts discounts "
        "LEFT JOIN analytics.order_line_items order_lines ON discounts.order_id=order_lines.order_unique_id "
        "GROUP BY discounts.code ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift])
@pytest.mark.query
def test_query_average_with_sql(connection, query_type: str):
    query = connection.get_sql_query(
        metrics=["average_order_value"], dimensions=["channel"], query_type=query_type
    )

    if query_type in {Definitions.snowflake}:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_average_order_value DESC NULLS LAST"
    elif query_type == Definitions.redshift:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (FARMFINGERPRINT64(orders.id)"
            ")::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (FARMFINGERPRINT64(orders.id))"
            "::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_average_order_value DESC NULLS LAST"
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
        order_by = ""

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        f"({sa_sum} / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.revenue)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0))"
        " as orders_average_order_value FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        f"GROUP BY {'order_lines.sales_channel' if query_type != Definitions.bigquery else 'order_lines_channel'}"  # noqa
        f"{order_by};"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.bigquery, Definitions.snowflake, Definitions.redshift])
@pytest.mark.query
def test_query_number_with_sql(connection, query_type):
    query = connection.get_sql_query(
        metrics=["customers.total_sessions_divide"], dimensions=["channel"], query_type=query_type
    )

    if query_type in {Definitions.snowflake}:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when (customers.is_churned)=false then customers.total_sessions end, 0) "  # noqa
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(case when (customers.is_churned)=false then customers.customer_id end), "  # noqa
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(case when (customers.is_churned)=false then customers.customer_id end), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "  # noqa
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY customers_total_sessions_divide DESC NULLS LAST"
    elif query_type == Definitions.redshift:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when (customers.is_churned)=false then customers.total_sessions end, 0) "  # noqa
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (FARMFINGERPRINT64(case when (customers.is_churned)=false then customers.customer_id end)"  # noqa
            ")::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (FARMFINGERPRINT64(case when (customers.is_churned)=false then customers.customer_id end))"  # noqa
            "::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY customers_total_sessions_divide DESC NULLS LAST"
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when (customers.is_churned)=false then customers.total_sessions end, 0) "  # noqa
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(CAST(case when (customers.is_churned)=false then customers.customer_id end AS STRING)) AS BIGNUMERIC)) "  # noqa
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(CAST(case when (customers.is_churned)=false then customers.customer_id end AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "  # noqa
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
        order_by = ""

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        f"({sa_sum}) / (100 * 1.0)"
        " as customers_total_sessions_divide FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        f"GROUP BY {'order_lines.sales_channel' if query_type != Definitions.bigquery else 'order_lines_channel'}"  # noqa
        f"{order_by};"
    )
    assert query == correct


@pytest.mark.query
def test_query_with_corrected_no_symm_agg_triggered(connection):
    query = connection.get_sql_query(
        metrics=["monthly_aggregates.count_new_employees"],
        dimensions=["monthly_aggregates.division"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2024, 1, 5, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_or_equal_than",
                "value": datetime.datetime(2024, 10, 5, 0, 0),
            },
        ],
        order_by=[{"field": "monthly_aggregates.division", "sort": "asc"}],
        limit=25,
    )

    # This, correctly, does not apply a symmetric aggregate
    correct = (
        "SELECT monthly_aggregates.division as"
        " monthly_aggregates_division,COUNT(monthly_aggregates.n_new_employees) as"
        " monthly_aggregates_count_new_employees FROM analytics.monthly_rollup monthly_aggregates WHERE"
        " DATE_TRUNC('DAY', monthly_aggregates.record_date)>='2024-01-05T00:00:00' AND DATE_TRUNC('DAY',"
        " monthly_aggregates.record_date)<='2024-10-05T00:00:00' GROUP BY monthly_aggregates.division ORDER"
        " BY monthly_aggregates_division ASC NULLS LAST LIMIT 25;"
    )
    assert query == correct
