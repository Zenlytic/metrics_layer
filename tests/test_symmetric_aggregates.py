import pytest

from metrics_layer.core.model.definitions import Definitions


@pytest.mark.query
def test_query_count_no_sql(connection):
    query = connection.get_sql_query(metrics=["number_of_customers"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,COUNT(DISTINCT(customers.customer_id))"
        " as customers_number_of_customers FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel ORDER BY customers_number_of_customers DESC;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift])
@pytest.mark.query
def test_query_sum_with_sql(connection, query_type):
    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["channel"], query_type=query_type)

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_total_revenue DESC"
    elif query_type == Definitions.bigquery:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(CAST(orders.id AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
        order_by = ""
    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        f"{sa} as orders_total_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        f"GROUP BY order_lines.sales_channel{order_by};"
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
        "GROUP BY order_lines.sales_channel ORDER BY orders_number_of_orders DESC;"
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
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics_live.discounts discounts ON orders.id=discounts.order_id "
        "GROUP BY discounts.code ORDER BY order_lines_number_of_email_purchased_items DESC;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift])
@pytest.mark.query
def test_query_average_with_sql(connection, query_type: str):
    query = connection.get_sql_query(
        metrics=["average_order_value"], dimensions=["channel"], query_type=query_type
    )

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY orders_average_order_value DESC"
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
        f"GROUP BY order_lines.sales_channel{order_by};"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.bigquery, Definitions.snowflake, Definitions.redshift])
@pytest.mark.query
def test_query_number_with_sql(connection, query_type):
    query = connection.get_sql_query(
        metrics=["total_sessions_divide"], dimensions=["channel"], query_type=query_type
    )

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when customers.is_churned=false then customers.total_sessions end, 0) "  # noqa
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(case when customers.is_churned=false then customers.customer_id end), "  # noqa
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(case when customers.is_churned=false then customers.customer_id end), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "  # noqa
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
        order_by = " ORDER BY customers_total_sessions_divide DESC"
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when customers.is_churned=false then customers.total_sessions end, 0) "  # noqa
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(CAST(case when customers.is_churned=false then customers.customer_id end AS STRING)) AS BIGNUMERIC)) "  # noqa
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(CAST(case when customers.is_churned=false then customers.customer_id end AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "  # noqa
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
        order_by = ""

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        f"({sa_sum}) / (100 * 1.0)"
        " as customers_total_sessions_divide FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        f"GROUP BY order_lines.sales_channel{order_by};"
    )
    assert query == correct
