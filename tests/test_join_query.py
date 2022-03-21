import pytest

from metrics_layer.core.model.project import AccessDeniedOrDoesNotExistException


@pytest.mark.query
def test_query_no_join_with_limit(connection):

    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"], limit=499)

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines GROUP BY order_lines.sales_channel "
        "ORDER BY order_lines_total_item_revenue DESC LIMIT 499;"
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

    assert query == "SUM(order_lines_total_item_revenue) / COUNT(orders_number_of_orders)"


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
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_average_order_revenue DESC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_dimension(connection):

    query = connection.get_sql_query(metrics=[], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id GROUP BY orders.new_vs_repeat "
        "ORDER BY orders_new_vs_repeat ASC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_costs DESC;"
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
        "ORDER BY orders_average_days_between_orders DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_functional_pk_resolve_one_to_many(connection):
    query = connection.get_sql_query(
        metrics=["discount_usd"],
        dimensions=["country"],
        explore_name="discounts_only",
    )

    correct = (
        "SELECT discounts.country as discounts_country,"
        "SUM(discount_detail.total_usd) as discount_detail_discount_usd "
        "FROM analytics_live.discounts discounts "
        "LEFT JOIN analytics.discount_detail discount_detail "
        "ON discounts.discount_id=discount_detail.discount_id "
        "GROUP BY discounts.country ORDER BY discount_detail_discount_usd DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_ensure_join_fields_are_respected(connection):
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_explore("order_lines_all")

        connection.get_sql_query(
            metrics=["discount_usd"],
            dimensions=["discount_promo_name"],
            explore_name="discounts_only",
        )

    assert exc_info.value


@pytest.mark.query
def test_query_single_join_count(connection):

    query = connection.get_sql_query(
        metrics=["order_lines.count"],
        dimensions=["channel", "new_vs_repeat"],
        explore_name="order_lines_all",
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "COUNT(order_lines.order_line_id) as order_lines_count FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat "
        "ORDER BY order_lines_count DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_metric_with_sub_field(connection):

    query = connection.get_sql_query(
        metrics=["line_item_aov"],
        dimensions=["channel"],
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) "
        "/ NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL "
        "THEN  orders.id  ELSE NULL END), 0) as order_lines_line_item_aov "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel "
        "ORDER BY order_lines_line_item_aov DESC;"
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
        "0)) as country_detail_avg_rainfall FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics_live.discounts discounts ON orders.id=discounts.order_id "
        "LEFT JOIN analytics.discount_detail discount_detail "
        "ON discounts.discount_id=discount_detail.discount_id "
        "AND DATE_TRUNC(CAST(discounts.order_date as DATE), WEEK) is not null "
        "LEFT JOIN (SELECT * FROM ANALYTICS.COUNTRY_DETAIL) as country_detail "
        "ON discounts.country=country_detail.country GROUP BY discount_detail.promo_name "
        "ORDER BY country_detail_avg_rainfall DESC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
    )

    assert query == correct


@pytest.mark.query
def test_query_single_join_with_case_raw_sql(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["is_on_sale_sql", "new_vs_repeat"],
    )

    correct = (
        "SELECT CASE WHEN order_lines.product_name ilike '%sale%' then TRUE else FALSE end "
        "as order_lines_is_on_sale_sql,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY CASE WHEN order_lines.product_name "
        "ilike '%sale%' then TRUE else FALSE end,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "ORDER BY order_lines_number_of_email_purchased_items DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC;"
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
        "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(case when customers.is_churned=false then "
        "customers.total_sessions end, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
        "+ (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "  # noqa
        "- SUM(DISTINCT (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) "
        "as customers_total_sessions "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY DATEDIFF('MONTH', orders.previous_order_date, orders.order_date) "
        "ORDER BY customers_total_sessions DESC;"
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
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region<>'West' "
        "GROUP BY customers.region,orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_where_literal(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where="first_order_week > '2021-07-12'",
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE DATE_TRUNC('WEEK', customers.first_order_date) > '2021-07-12' "
        "GROUP BY customers.region,orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC;"
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
        "ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_having_literal(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having="total_item_revenue > -12",
    )

    correct = (
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue) > -12 "
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "SELECT customers.region as customers_region,orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat ORDER BY total_item_revenue ASC;"
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
        "ORDER BY total_item_revenue ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_count_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["new_order_count"],
        dimensions=["channel"],
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,COUNT(DISTINCT("
        "case when orders.new_vs_repeat='New' then orders.id end)) "
        "as orders_new_order_count FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel ORDER BY orders_new_order_count DESC;"
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
        "split_part(listagg(order_lines.inventory_qty, ',') within group "
        "(order by DATE_TRUNC('DAY', order_lines.order_date) desc), ',', 0)::int "
        "as order_lines_ending_on_hand_qty "
        "FROM analytics.order_line_items order_lines GROUP BY order_lines.product_name "
        "ORDER BY order_lines_ending_on_hand_qty DESC;"
    )
    assert query == correct
