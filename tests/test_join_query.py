from datetime import datetime

import pytest

from metrics_layer.core.exceptions import JoinError, QueryError
from metrics_layer.core.model import Definitions


@pytest.mark.query
def test_query_no_join_with_limit(connection):
    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"], limit=499)

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines GROUP BY order_lines.sales_channel "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST LIMIT 499;"
    )
    assert query == correct


@pytest.mark.query
def test_alias_only_query(connection):
    metric = connection.get_metric(metric_name="total_item_revenue")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    assert query == "SUM(order_lines_total_item_revenue)"


@pytest.mark.query
def test_alias_only_query_number(connection):
    metric = connection.get_metric(metric_name="line_item_aov")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    assert query == "(SUM(order_lines_total_item_revenue)) / (COUNT(orders_number_of_orders))"


@pytest.mark.query
def test_alias_only_query_symmetric_average_distinct(connection):
    metric = connection.get_metric(metric_name="average_order_revenue")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    correct = (
        "(COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(order_lines_average_order_revenue, 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(order_lines_order_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(order_lines_order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS "
        "DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  (order_lines_average_order_revenue)  "
        "IS NOT NULL THEN  order_lines_order_id  ELSE NULL END), 0))"
    )
    assert query == correct


@pytest.mark.query
def test_query_no_join_average_distinct(connection):
    query = connection.get_sql_query(metrics=["average_order_revenue"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(order_lines.order_total, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
        "+ (TO_NUMBER(MD5(order_lines.order_unique_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_unique_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(order_lines.order_total)  IS NOT NULL THEN  order_lines.order_unique_id  ELSE NULL END), 0)) "
        "as order_lines_average_order_revenue FROM analytics.order_line_items order_lines "
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_average_order_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("field", ["order_lines.order_week", "orders.order_week"])
def test_query_bigquery_week_filter_type_conversion(connection, field):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["channel"],
        where=[
            {
                "field": field,
                "expression": "greater_than",
                "value": datetime(year=2021, month=8, day=4),
            }
        ],
        query_type="BIGQUERY",
    )

    cast_as = "DATE" if "order_lines.order_week" == field else "TIMESTAMP"
    if cast_as == "DATE":
        casted = f"CAST(CAST('2021-08-04 00:00:00' AS TIMESTAMP) AS {cast_as})"
    else:
        casted = f"CAST('2021-08-04 00:00:00' AS {cast_as})"
    sql_field = "order_lines.order_date" if "order_lines.order_week" == field else "orders.order_date"
    join = ""
    if "orders" in field:
        join = "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        f" order_lines_total_item_revenue FROM analytics.order_line_items order_lines {join}WHERE"
        f" CAST(DATE_TRUNC(CAST({sql_field} AS DATE), WEEK) AS {cast_as})>{casted} GROUP BY"
        " order_lines_channel;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join(connection):
    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel", "new_vs_repeat"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_dimension(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat FROM "
        "analytics.orders orders GROUP BY orders.new_vs_repeat "
        "ORDER BY orders_new_vs_repeat ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_dimension_with_comment(connection):
    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["parent_channel"])

    correct = (
        "SELECT CASE\n--- parent channel\nWHEN order_lines.sales_channel ilike '%social%' then "
        "'Social'\nELSE 'Not Social'\nEND as order_lines_parent_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines GROUP BY CASE\n--- parent channel\nWHEN "
        "order_lines.sales_channel ilike '%social%' then 'Social'\nELSE 'Not Social'\nEND "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_dimension_with_multi_filter(connection):
    query = connection.get_sql_query(metrics=["total_item_costs"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(case when order_lines.product_name"
        "='Portable Charger' and order_lines.product_name IN ('Portable Charger','Dual Charger') "
        "and orders.revenue * 100>100 then order_lines.item_costs end) "
        "as order_lines_total_item_costs FROM analytics.order_line_items order_lines LEFT JOIN "
        "analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_costs DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_dimension_sa_duration(connection):
    query = connection.get_sql_query(metrics=["average_days_between_orders"], dimensions=["product_name"])

    correct = (
        "SELECT order_lines.product_name as order_lines_product_name,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(DATEDIFF('DAY', orders.previous_order_date, orders.order_date), 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) "
        "/ NULLIF(COUNT(DISTINCT CASE WHEN  (DATEDIFF('DAY', orders.previous_order_date, "
        "orders.order_date))  IS NOT NULL THEN  orders.id  "
        "ELSE NULL END), 0)) as orders_average_days_between_orders "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY order_lines.product_name "
        "ORDER BY orders_average_days_between_orders DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_functional_pk_resolve_one_to_many(connection):
    query = connection.get_sql_query(
        metrics=["discount_usd"],
        dimensions=["country"],
    )

    correct = (
        "SELECT discounts.country as discounts_country,"
        "SUM(discount_detail.total_usd) as discount_detail_discount_usd "
        "FROM analytics.discount_detail discount_detail "
        "LEFT JOIN analytics_live.discounts discounts ON "
        "discounts.discount_id=discount_detail.discount_id "
        "AND DATE_TRUNC('WEEK', CAST(discounts.order_date AS DATE)) is not null "
        "GROUP BY discounts.country ORDER BY discount_detail_discount_usd DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_ensure_join_fields_are_respected_two(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_sessions"],
            dimensions=["total_item_revenue"],
        )

    assert exc_info.value


@pytest.mark.query
def test_ensure_join_fields_are_respected_three_or_more(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_sessions"],
            dimensions=["total_item_revenue", "new_vs_repeat", "gender"],
        )

    assert exc_info.value


@pytest.mark.query
def test_ensure_only_join_is_respected(fresh_project):
    # This is valid and the connection exists in the join graph
    fresh_project.join_graph.graph["order_lines"]["orders"]

    # Once we tell to only use the discounts view, the above will no longer exist
    fresh_project._views[0]["identifiers"][1]["only_join"] = ["discounts"]
    new_graph = fresh_project.join_graph.build()

    with pytest.raises(KeyError) as exc_info:
        new_graph["order_lines"]["orders"]

    assert exc_info.value


@pytest.mark.query
def test_query_single_join_metric_with_sub_field(connection):
    query = connection.get_sql_query(
        metrics=["line_item_aov"],
        dimensions=["channel"],
    )

    correct = (
        "WITH order_lines_order__cte_subquery_0 AS (SELECT order_lines.sales_channel as "
        "order_lines_channel,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines GROUP BY order_lines.sales_channel "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        "orders_order__cte_subquery_1 AS (SELECT order_lines.sales_channel as order_lines_channel,"
        "NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN  orders.id  "
        "ELSE NULL END), 0) as orders_number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY "
        "order_lines.sales_channel ORDER BY orders_number_of_orders DESC NULLS LAST) "
        "SELECT order_lines_order__cte_subquery_0.order_lines_total_item_revenue as "
        "order_lines_total_item_revenue,orders_order__cte_subquery_1.orders_number_of_orders "
        "as orders_number_of_orders,ifnull(order_lines_order__cte_subquery_0.order_lines_channel, "
        "orders_order__cte_subquery_1.order_lines_channel) as order_lines_channel,"
        "order_lines_total_item_revenue / orders_number_of_orders as order_lines_line_item_aov "
        "FROM order_lines_order__cte_subquery_0 FULL OUTER JOIN orders_order__cte_subquery_1 "
        "ON order_lines_order__cte_subquery_0.order_lines_channel"
        "=orders_order__cte_subquery_1.order_lines_channel;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_with_forced_additional_join(connection):
    query = connection.get_sql_query(
        metrics=["avg_rainfall"],
        dimensions=["discount_promo_name"],
        query_type="BIGQUERY",
    )

    correct = (
        "SELECT discount_detail.promo_name as discount_detail_discount_promo_name,(COALESCE(CAST(("
        "SUM(DISTINCT (CAST(FLOOR(COALESCE(country_detail.rain, 0) * (1000000 * 1.0)) AS FLOAT64))"
        " + CAST(FARM_FINGERPRINT(CAST(country_detail.country AS STRING)) AS BIGNUMERIC)) - SUM(DISTINCT "
        "CAST(FARM_FINGERPRINT(CAST(country_detail.country AS STRING)) AS BIGNUMERIC))) AS FLOAT64) "
        "/ CAST((1000000*1.0) AS FLOAT64), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(country_detail.rain)  IS NOT NULL THEN  country_detail.country  ELSE NULL END), "
        "0)) as country_detail_avg_rainfall FROM analytics.discount_detail discount_detail "
        "LEFT JOIN analytics_live.discounts discounts ON discounts.discount_id=discount_detail.discount_id "
        "AND CAST(DATE_TRUNC(CAST(discounts.order_date AS DATE), WEEK) AS TIMESTAMP) is not null LEFT JOIN "
        "(SELECT * FROM ANALYTICS.COUNTRY_DETAIL) as country_detail "
        "ON discounts.country=country_detail.country "
        "GROUP BY discount_detail_discount_promo_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_select_args(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["channel", "new_vs_repeat"],
        select_raw_sql=[
            "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1",
            "CAST(date_created > '2021-04-02' AS INT) as period",
        ],
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue,"
        "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1,"
        "CAST(date_created > '2021-04-02' AS INT) as period FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat,"
        "CAST(new_vs_repeat = 'Repeat' AS INT),CAST(date_created > '2021-04-02' AS INT) "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )

    assert query == correct


@pytest.mark.query
def test_query_single_join_with_case_raw_sql(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["is_on_sale_sql", "new_vs_repeat"],
    )

    correct = (
        "SELECT (CASE WHEN order_lines.product_name ilike '%sale%' then TRUE else FALSE end) "
        "as order_lines_is_on_sale_sql,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY (CASE WHEN order_lines.product_name "
        "ilike '%sale%' then TRUE else FALSE end),orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_with_case(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["is_on_sale_case", "new_vs_repeat"],
    )

    correct = (
        "SELECT case when order_lines.product_name ilike '%sale%' then 'On sale' else 'Not on sale' end "  # noqa
        "as order_lines_is_on_sale_case,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY case when order_lines.product_name "
        "ilike '%sale%' then 'On sale' else 'Not on sale' end,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_with_tier(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_tier", "new_vs_repeat"],
    )

    tier_case_query = "case when order_lines.revenue < 0 then 'Below 0' when order_lines.revenue >= 0 "
    tier_case_query += "and order_lines.revenue < 20 then '[0,20)' when order_lines.revenue >= 20 and "
    tier_case_query += "order_lines.revenue < 50 then '[20,50)' when order_lines.revenue >= 50 and "
    tier_case_query += "order_lines.revenue < 100 then '[50,100)' when order_lines.revenue >= 100 and "
    tier_case_query += "order_lines.revenue < 300 then '[100,300)' when order_lines.revenue >= 300 "
    tier_case_query += "then '[300,inf)' else 'Unknown' end"

    correct = (
        f"SELECT {tier_case_query} as order_lines_order_tier,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        f"ON order_lines.order_unique_id=orders.id GROUP BY {tier_case_query},orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_with_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_email_purchased_items"],
        dimensions=["channel", "new_vs_repeat"],
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "COUNT(case when order_lines.sales_channel='Email' then order_lines.order_id end) "
        "as order_lines_number_of_email_purchased_items FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id"
        " GROUP BY order_lines.sales_channel,orders.new_vs_repeat "
        "ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_with_custom_join_type(connection):
    query = connection.get_sql_query(
        metrics=["submitted_form.number_of_form_submissions"],
        dimensions=["submitted_form.context_os", "customers.gender"],
    )

    correct = (
        "SELECT submitted_form.context_os as submitted_form_context_os,customers.gender as"
        " customers_gender,COUNT(submitted_form.id) as submitted_form_number_of_form_submissions FROM"
        " analytics.submitted_form submitted_form FULL OUTER JOIN analytics.customers customers ON"
        " customers.customer_id=submitted_form.customer_id AND DATE_TRUNC('DAY', submitted_form.session_date)"
        " is not null GROUP BY submitted_form.context_os,customers.gender ORDER BY"
        " submitted_form_number_of_form_submissions DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as"
        " orders_new_vs_repeat,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id GROUP BY customers.region,orders.new_vs_repeat ORDER"
        " BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_quad_join(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat", "discount_code"],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "discounts.code as discounts_discount_code,COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)"
        "::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / "
        "CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as order_lines_total_item_revenue "
        "FROM analytics_live.discounts discounts LEFT JOIN analytics.order_line_items order_lines "
        "ON discounts.order_id=order_lines.order_unique_id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "LEFT JOIN analytics.orders orders ON orders.id=discounts.order_id "
        "GROUP BY customers.region,orders.new_vs_repeat,discounts.code "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_with_duration(connection):
    query = connection.get_sql_query(
        metrics=["total_sessions"],
        dimensions=["months_between_orders"],
    )

    correct = (
        "SELECT DATEDIFF('MONTH', orders.previous_order_date, orders.order_date) as orders_months_between_orders,"  # noqa
        "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when (customers.is_churned)=false then "
        "customers.total_sessions end, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
        "+ (TO_NUMBER(MD5(case when (customers.is_churned)=false then customers.customer_id end), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "  # noqa
        "- SUM(DISTINCT (TO_NUMBER(MD5(case when (customers.is_churned)=false then customers.customer_id end), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "  # noqa
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) "
        "as customers_total_sessions "
        "FROM analytics.orders orders "
        "LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id "
        "GROUP BY DATEDIFF('MONTH', orders.previous_order_date, orders.order_date) "
        "ORDER BY customers_total_sessions DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_where_dict(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as"
        " orders_new_vs_repeat,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id WHERE customers.region<>'West' GROUP BY"
        " customers.region,orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_where_literal(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where="${customers.first_order_week} > '2021-07-12'",
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as"
        " orders_new_vs_repeat,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id WHERE DATE_TRUNC('WEEK',"
        " CAST(customers.first_order_date AS DATE)) > '2021-07-12' GROUP BY"
        " customers.region,orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_having_dict(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12 "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_having_literal(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having="${total_item_revenue} > -12",
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING (SUM(order_lines.revenue)) > -12 "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_order_by_literal(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        order_by="total_item_revenue",
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as"
        " orders_new_vs_repeat,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id GROUP BY customers.region,orders.new_vs_repeat ORDER"
        " BY order_lines_total_item_revenue ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_all(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
        order_by=[{"field": "total_item_revenue", "sort": "asc"}],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region<>'West' "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12 "
        "ORDER BY order_lines_total_item_revenue ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_count_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["new_order_count"],
        dimensions=["channel"],
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,NULLIF(COUNT(DISTINCT"
        " CASE WHEN  (case when orders.new_vs_repeat='New' then orders.id end)  IS "
        "NOT NULL THEN  case when orders.new_vs_repeat='New' then orders.id end  "
        "ELSE NULL END), 0) "
        "as orders_new_order_count FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel ORDER BY orders_new_order_count DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_implicit_add_three_views(connection):
    query = connection.get_sql_query(
        metrics=["number_of_customers"],
        dimensions=["discount_code", "rainfall"],
    )

    correct = (
        "SELECT discounts.code as discounts_discount_code,country_detail.rain "
        "as country_detail_rainfall,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(customers.customer_id)  IS NOT NULL THEN  customers.customer_id  ELSE NULL END), 0) "
        "as customers_number_of_customers FROM analytics_live.discounts discounts "
        "LEFT JOIN (SELECT * FROM ANALYTICS.COUNTRY_DETAIL) as country_detail "
        "ON discounts.country=country_detail.country "
        "LEFT JOIN analytics.orders orders ON orders.id=discounts.order_id "
        "LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id "
        "GROUP BY discounts.code,country_detail.rain "
        "ORDER BY customers_number_of_customers DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_number_measure_w_dimension_reference(connection):
    query = connection.get_sql_query(
        metrics=["ending_on_hand_qty"],
        dimensions=["product_name"],
    )

    correct = (
        "SELECT order_lines.product_name as order_lines_product_name,"
        "split_part(listagg((order_lines.inventory_qty), ',') within group "
        "(order by (DATE_TRUNC('DAY', order_lines.order_date)) desc), ',', 0)::int "
        "as order_lines_ending_on_hand_qty "
        "FROM analytics.order_line_items order_lines GROUP BY order_lines.product_name "
        "ORDER BY order_lines_ending_on_hand_qty DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_number_as_array_filter(connection):
    query = connection.get_sql_query(
        metrics=["total_non_merchant_revenue"],
        dimensions=[],
        where=[
            {"field": "orders.order_date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    correct = (
        "SELECT SUM(case when orders.anon_id NOT IN (9,3,22,9082) then orders.revenue end) as"
        " orders_total_non_merchant_revenue FROM analytics.orders orders WHERE DATE_TRUNC('DAY',"
        " orders.order_date)>'2022-04-03' ORDER BY orders_total_non_merchant_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("bool_value", ["True", "False"])
def test_query_bool_and_date_filter(connection, bool_value):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["channel"],
        where=[
            {"field": "is_churned", "expression": "equal_to", "value": bool_value},
            {"field": "order_lines.order_date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    if bool_value == "True":
        negation = ""
    else:
        negation = "NOT "
    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        f"WHERE {negation}(customers.is_churned) AND DATE_TRUNC('DAY', order_lines.order_date)>'2022-04-03' "
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("filter_type", ["having", "where"])
def test_query_sub_group_by_filter(connection, filter_type):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["region"],
        **{
            filter_type: [
                {
                    "field": "total_item_revenue",
                    "group_by": "customers.customer_id",
                    "expression": "greater_than",
                    "value": 1000,
                }
            ]
        },
    )

    correct = (
        "WITH filter_subquery_0 AS (SELECT customers.customer_id as customers_customer_id "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
        "ON order_lines.customer_id=customers.customer_id GROUP BY customers.customer_id "
        "HAVING SUM(order_lines.revenue)>1000 ORDER BY customers_customer_id ASC NULLS LAST) "
        "SELECT customers.region as customers_region,COUNT(orders.id) as orders_number_of_orders "
        "FROM analytics.orders orders LEFT JOIN analytics.customers customers "
        "ON orders.customer_id=customers.customer_id "
        "WHERE customers.customer_id IN (SELECT DISTINCT customers_customer_id "
        "FROM filter_subquery_0) GROUP BY customers.region "
        "ORDER BY orders_number_of_orders DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_sum_when_should_be_number(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["should_be_number"],
            dimensions=["region"],
        )

    error_message = (
        "Field should_be_number has the wrong type. You must use the type 'number' if you reference "
        "other measures in your expression (like line_item_aov referenced here)"
    )
    assert isinstance(exc_info.value, QueryError)
    assert str(exc_info.value) == error_message


@pytest.mark.query
def test_join_graph_working_as_expected(connection):
    query = connection.get_sql_query(
        metrics=["number_of_clicks"],
        dimensions=["date", "context_os"],
    )

    correct = (
        "SELECT DATE_TRUNC('DAY', clicked_on_page.session_date) as clicked_on_page_session_date,"
        "clicked_on_page.context_os as clicked_on_page_context_os,"
        "COUNT(clicked_on_page.id) as clicked_on_page_number_of_clicks "
        "FROM analytics.clicked_on_page clicked_on_page "
        "GROUP BY DATE_TRUNC('DAY', clicked_on_page.session_date),"
        "clicked_on_page.context_os ORDER BY clicked_on_page_number_of_clicks DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_graph_many_to_many_use_bridge_table(connection):
    query = connection.get_sql_query(metrics=["number_of_customers"], dimensions=["accounts.account_name"])

    correct = (
        "SELECT accounts.name as accounts_account_name,NULLIF(COUNT(DISTINCT CASE WHEN "
        " (customers.customer_id)  IS NOT NULL THEN  customers.customer_id  ELSE NULL END), 0) as"
        " customers_number_of_customers FROM analytics.customer_accounts z_customer_accounts LEFT JOIN"
        " analytics.accounts accounts ON z_customer_accounts.account_id=accounts.account_id LEFT JOIN"
        " analytics.customers customers ON z_customer_accounts.customer_id=customers.customer_id GROUP BY"
        " accounts.name ORDER BY customers_number_of_customers DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_graph_many_to_many_skip_bridge_table(connection):
    query = connection.get_sql_query(
        metrics=["number_of_customers", "number_of_orders"],
        dimensions=["accounts.account_name"],
    )

    correct = (
        "SELECT accounts.name as accounts_account_name,NULLIF(COUNT(DISTINCT CASE WHEN "
        " (customers.customer_id)  IS NOT NULL THEN  customers.customer_id  ELSE NULL END), 0) as"
        " customers_number_of_customers,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders"
        " orders LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id LEFT"
        " JOIN analytics.accounts accounts ON orders.account_id=accounts.account_id GROUP BY accounts.name"
        " ORDER BY customers_number_of_customers DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_graph_raise_unjoinable_error(connection):
    with pytest.raises(JoinError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_clicks"], dimensions=["date", "submitted_form.context_os"], single_query=True
        )

    error_message = (
        "There was no join path between the views: ['clicked_on_page', 'submitted_form']. Check the "
        "identifiers on your views and make sure they are joinable."
    )
    assert isinstance(exc_info.value, JoinError)
    assert str(exc_info.value) == error_message


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type",
    [
        Definitions.snowflake,
        Definitions.druid,
        Definitions.trino,
        Definitions.redshift,
        Definitions.bigquery,
        Definitions.sql_server,
        Definitions.azure_synapse,
        Definitions.duck_db,
    ],
)
def test_median_aggregate_function(connection, query_type):
    if query_type in [Definitions.snowflake, Definitions.redshift, Definitions.duck_db]:
        query = connection.get_sql_query(
            metrics=["median_customer_ltv"], dimensions=[], query_type=query_type
        )
        correct = (
            "SELECT MEDIAN(customers.customer_ltv) as customers_median_customer_ltv "
            "FROM analytics.customers customers ORDER BY customers_median_customer_ltv DESC NULLS LAST;"
        )
        assert query == correct
    else:
        with pytest.raises(QueryError) as exc_info:
            connection.get_sql_query(metrics=["median_customer_ltv"], dimensions=[], query_type=query_type)

        error_message = (
            f"Median is not supported in {query_type}. Please choose another "
            "aggregate function for the customers.median_customer_ltv measure."
        )
        assert isinstance(exc_info.value, QueryError)
        assert str(exc_info.value) == error_message


@pytest.mark.query
def test_always_filter_with_and_without_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_workspace_creations"],
        dimensions=["date"],
    )

    correct = (
        "SELECT DATE_TRUNC('DAY', created_workspace.session_date) as"
        " created_workspace_created_date,COUNT(created_workspace.id) as"
        " created_workspace_number_of_workspace_creations FROM analytics.created_workspace created_workspace"
        " LEFT JOIN analytics.customers customers ON created_workspace.customer_id=customers.customer_id"
        " WHERE NOT (customers.is_churned) AND NOT created_workspace.context_os IS NULL AND"
        " created_workspace.context_os IN ('1','Google','os:iOS') AND created_workspace.id NOT IN (1,44,87)"
        " GROUP BY DATE_TRUNC('DAY', created_workspace.session_date) ORDER BY"
        " created_workspace_number_of_workspace_creations DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_as_ability_single_join_non_as(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["accounts.account_name"],
    )

    correct = (
        "SELECT accounts.name as accounts_account_name,"
        "COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts "
        "FROM analytics.mrr_by_customer mrr LEFT JOIN analytics.accounts accounts "
        "ON mrr.account_id=accounts.account_id "
        "GROUP BY accounts.name ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_as_ability_single_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["parent_account.account_name"],
    )

    correct = (
        "SELECT parent_account.name as parent_account_account_name,"
        "COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts "
        "FROM analytics.mrr_by_customer mrr LEFT JOIN analytics.accounts parent_account "
        "ON mrr.parent_account_id=parent_account.account_id "
        "GROUP BY parent_account.name ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_as_ability_exclude_metrics(connection):
    # This one should have metrics because of the include_metrics flag
    assert any(f.name == "n_created_accounts" for f in connection.project.fields(view_name="parent_account"))

    # This one should NOT have metrics because the include_metrics flag defaults to false
    assert not any(
        f.name == "n_created_accounts" for f in connection.project.fields(view_name="child_account")
    )


@pytest.mark.query
def test_join_as_ability_single_join_as_and_non_as(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["parent_account.account_name", "accounts.account_name"],
    )

    correct = (
        "SELECT parent_account.name as parent_account_account_name,"
        "accounts.name as accounts_account_name,"
        "COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts "
        "FROM analytics.mrr_by_customer mrr "
        "LEFT JOIN analytics.accounts accounts ON mrr.account_id=accounts.account_id "
        "LEFT JOIN analytics.accounts parent_account "
        "ON mrr.parent_account_id=parent_account.account_id "
        "GROUP BY parent_account.name,accounts.name ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )

    assert query == correct


@pytest.mark.query
def test_join_as_ability_single_join_only_where(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=[],
        where=[
            {"field": "parent_account.account_name", "expression": "not_equal_to", "value": "Amazon"},
            {"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"},
        ],
    )

    correct = (
        "SELECT COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts "
        "FROM analytics.mrr_by_customer mrr "
        "LEFT JOIN analytics.accounts parent_account "
        "ON mrr.parent_account_id=parent_account.account_id "
        "WHERE parent_account.name<>'Amazon' AND mrr.plan_name='Enterprise' "
        "ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )

    assert query == correct


@pytest.mark.query
def test_join_as_ability_single_join_as_and_non_as_extra_dims(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["parent_account.account_name", "accounts.account_name", "date", "mrr.plan_name"],
        where=[
            {"field": "parent_account.account_name", "expression": "not_equal_to", "value": "Amazon"},
            {"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"},
        ],
    )

    correct = (
        "SELECT parent_account.name as parent_account_account_name,"
        "accounts.name as accounts_account_name,"
        "DATE_TRUNC('DAY', mrr.record_date) as mrr_record_date,"
        "mrr.plan_name as mrr_plan_name,"
        "COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts "
        "FROM analytics.mrr_by_customer mrr "
        "LEFT JOIN analytics.accounts accounts ON mrr.account_id=accounts.account_id "
        "LEFT JOIN analytics.accounts parent_account "
        "ON mrr.parent_account_id=parent_account.account_id "
        "WHERE parent_account.name<>'Amazon' AND mrr.plan_name='Enterprise' "
        "GROUP BY parent_account.name,accounts.name,DATE_TRUNC('DAY', mrr.record_date),"
        "mrr.plan_name ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )

    assert query == correct


@pytest.mark.query
def test_join_as_ability_double_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["parent_account.account_name", "child_account.account_name"],
    )

    correct = (
        "SELECT parent_account.name as parent_account_account_name,child_account.name as"
        " child_account_account_name,COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts FROM"
        " analytics.mrr_by_customer mrr LEFT JOIN analytics.accounts parent_account ON"
        " mrr.parent_account_id=parent_account.account_id LEFT JOIN analytics.accounts child_account ON"
        " mrr.child_account_id=child_account.account_id GROUP BY parent_account.name,child_account.name ORDER"
        " BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_null_filter_handling_metric_filter(connection):
    query = connection.get_sql_query(metrics=["number_of_acquired_accounts_missing"], dimensions=[])

    correct = (
        "SELECT COUNT(case when aa_acquired_accounts.account_id IS NULL then "
        "aa_acquired_accounts.account_id end) as aa_acquired_accounts_number_of_acquired_accounts_missing "
        "FROM analytics.accounts aa_acquired_accounts "
        "ORDER BY aa_acquired_accounts_number_of_acquired_accounts_missing DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_join_graph_production_with_sql_reference(connection):
    sessions_field = connection.project.get_field("unique_user_iphone_sessions")
    sessions_no_merged_results = [jg for jg in sessions_field.join_graphs() if "merged_result" not in jg]

    revenue_field = connection.project.get_field("total_item_revenue")
    revenue_no_merged_results = [jg for jg in revenue_field.join_graphs() if "merged_result" not in jg]

    # These should NOT overlap in join graphs (without merged results) because the
    # first (though on the customers view), references a field in the sessions view,
    # which requires a join that the revenue metric does not have as an option
    assert set(revenue_no_merged_results).isdisjoint(sessions_no_merged_results)


@pytest.mark.query
def test_join_as_label(connection):
    view = connection.project.get_view("child_account")
    assert view.name == "child_account"
    assert view.label == "Sub Account"
    assert view.fields()[0].label == "Sub Account Account Id"

    view = connection.project.get_view("parent_account")
    assert view.name == "parent_account"
    assert view.fields()[0].label == "Parent Account Id"


@pytest.mark.query
def test_join_as_label_field_level(connection):
    child_account_id = connection.project.get_field("child_account.account_id")
    assert child_account_id.name == "account_id"
    assert child_account_id.label_prefix == "Sub Account"
    assert child_account_id.label == "Sub Account Account Id"

    parent_account_id = connection.project.get_field("parent_account.account_id")
    assert parent_account_id.name == "account_id"
    assert parent_account_id.label_prefix == "Parent"
    assert parent_account_id.label == "Parent Account Id"

    child_account_name = connection.project.get_field("child_account.account_name")
    assert child_account_name.name == "account_name"
    assert child_account_name.label_prefix == "Sub Account"
    assert child_account_name.label == "Sub Account Account Name"

    parent_account_name = connection.project.get_field("parent_account.account_name")
    assert parent_account_name.name == "account_name"
    assert parent_account_name.label_prefix == "Parent"
    assert parent_account_name.label == "Parent Account Name"
