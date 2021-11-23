# import pytest


def test_query_no_join(connection):

    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) as total_item_revenue FROM "
    )
    correct += "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel;"
    assert query == correct


def test_alias_only_query(connection):
    metric = connection.get_metric(metric_name="total_item_revenue")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    assert query == "SUM(total_item_revenue)"


def test_alias_only_query_number(connection):
    metric = connection.get_metric(metric_name="line_item_aov")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    assert query == "SUM(total_item_revenue) / COUNT(number_of_orders)"


def test_alias_only_query_symmetric_average_distinct(connection):
    metric = connection.get_metric(metric_name="average_order_revenue")
    query = metric.sql_query(query_type="SNOWFLAKE", alias_only=True)

    correct = (
        "(COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(average_order_revenue, 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(order_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS "
        "DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  (average_order_revenue)  "
        "IS NOT NULL THEN  order_id  ELSE NULL END), 0))"
    )
    assert query == correct


def test_query_no_join_average_distinct(connection):

    query = connection.get_sql_query(metrics=["average_order_revenue"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as channel,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(order_lines.order_total, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
        "+ (TO_NUMBER(MD5(order_lines.order_unique_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_unique_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(order_lines.order_total)  IS NOT NULL THEN  order_lines.order_unique_id  ELSE NULL END), 0)) "
        "as average_order_revenue FROM analytics.order_line_items order_lines "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_single_join(connection):

    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel", "new_vs_repeat"])

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += (
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_single_dimension(connection):

    query = connection.get_sql_query(metrics=[], dimensions=["new_vs_repeat"])

    correct = "SELECT orders.new_vs_repeat as new_vs_repeat FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += "order_lines.order_unique_id=orders.id GROUP BY orders.new_vs_repeat;"
    assert query == correct


def test_query_single_dimension_with_comment(connection):
    query = connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["parent_channel"])

    correct = (
        "SELECT CASE\n--- parent channel\nWHEN order_lines.sales_channel ilike '%social%' then "
        "'Social'\nELSE 'Not Social'\nEND as parent_channel,SUM(order_lines.revenue) as total_item_revenue "
        "FROM analytics.order_line_items order_lines GROUP BY CASE\n--- parent channel\nWHEN "
        "order_lines.sales_channel ilike '%social%' then 'Social'\nELSE 'Not Social'\nEND;"
    )
    assert query == correct


def test_query_single_dimension_with_multi_filter(connection):
    query = connection.get_sql_query(metrics=["total_item_costs"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as channel,SUM(case when order_lines.product_name "
        "= 'Portable Charger' and orders.revenue * 100 > 100 then order_lines.item_costs end) "
        "as total_item_costs FROM analytics.order_line_items order_lines LEFT JOIN "
        "analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_single_dimension_sa_duration(connection):
    query = connection.get_sql_query(metrics=["average_days_between_orders"], dimensions=["product_name"])

    correct = (
        "SELECT order_lines.product_name as product_name,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(DATEDIFF('DAY', orders.previous_order_date, orders.order_date), 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) "
        "/ NULLIF(COUNT(DISTINCT CASE WHEN  (DATEDIFF('DAY', orders.previous_order_date, "
        "orders.order_date))  IS NOT NULL THEN  orders.id  "
        "ELSE NULL END), 0)) as average_days_between_orders "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY order_lines.product_name;"
    )
    assert query == correct


def test_query_single_join_count(connection):

    query = connection.get_sql_query(
        metrics=["order_lines.count"],
        dimensions=["channel", "new_vs_repeat"],
        explore_name="order_lines",
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "COUNT(order_lines.order_line_id) as count FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += (
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_single_join_metric_with_sub_field(connection):

    query = connection.get_sql_query(
        metrics=["line_item_aov"],
        dimensions=["channel"],
    )

    correct = (
        "SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) "
        "/ NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL "
        "THEN  orders.id  ELSE NULL END), 0) as line_item_aov "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_single_join_with_forced_additional_join(connection):
    query = connection.get_sql_query(
        metrics=["avg_rainfall"],
        dimensions=["discount_promo_name"],
        query_type="BIGQUERY",
    )

    correct = (
        "SELECT discount_detail.promo_name as discount_promo_name,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(country_detail.rain, 0) * (1000000 * 1.0)) AS FLOAT64))"
        " + CAST(FARM_FINGERPRINT(country_detail.country) AS BIGNUMERIC)) - SUM(DISTINCT "
        "CAST(FARM_FINGERPRINT(country_detail.country) AS BIGNUMERIC))) AS FLOAT64) "
        "/ CAST((1000000*1.0) AS FLOAT64), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(country_detail.rain)  IS NOT NULL THEN  country_detail.country  ELSE NULL END), "
        "0)) as avg_rainfall FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics_live.discounts discounts ON orders.id=discounts.order_id "
        "LEFT JOIN analytics.discount_detail discount_detail ON discounts.discount_id=discount_detail.discount_id "  # noqa
        "LEFT JOIN (SELECT * FROM ANALYTICS.COUNTRY_DETAIL) as country_detail "
        "ON discounts.country=country_detail.country GROUP BY discount_detail.promo_name;"
    )
    assert query == correct


def test_query_single_join_select_args(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["channel", "new_vs_repeat"],
        select_raw_sql=[
            "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1",
            "CAST(date_created > '2021-04-02' AS INT) as period",
        ],
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue,"
    correct += "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1,"
    correct += "CAST(date_created > '2021-04-02' AS INT) as period FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += (
        "order_lines.order_unique_id=orders.id GROUP BY order_lines.sales_channel,orders.new_vs_repeat,"
    )
    correct += "CAST(new_vs_repeat = 'Repeat' AS INT),CAST(date_created > '2021-04-02' AS INT);"

    assert query == correct


def test_query_single_join_with_case_raw_sql(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["is_on_sale_sql", "new_vs_repeat"],
    )

    correct = "SELECT CASE WHEN order_lines.product_name ilike '%sale%' then TRUE else FALSE end "
    correct += "as is_on_sale_sql,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_unique_id=orders.id GROUP BY CASE WHEN order_lines.product_name "
    correct += "ilike '%sale%' then TRUE else FALSE end,orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_with_case(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["is_on_sale_case", "new_vs_repeat"],
    )

    correct = "SELECT case when order_lines.product_name ilike '%sale%' then 'On sale' else 'Not on sale' end "  # noqa
    correct += "as is_on_sale_case,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_unique_id=orders.id GROUP BY case when order_lines.product_name "
    correct += "ilike '%sale%' then 'On sale' else 'Not on sale' end,orders.new_vs_repeat;"
    assert query == correct


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

    correct = f"SELECT {tier_case_query} as order_tier,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += f"ON order_lines.order_unique_id=orders.id GROUP BY {tier_case_query},orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_with_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_email_purchased_items"],
        dimensions=["channel", "new_vs_repeat"],
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "COUNT(case when order_lines.sales_channel = 'Email' then order_lines.order_id end) "
    correct += "as number_of_email_purchased_items FROM analytics.order_line_items "
    correct += "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id"
    correct += " GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    assert query == correct


def test_query_multiple_join(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_multiple_join_where_dict(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region<>'West' "
        "GROUP BY customers.region,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_multiple_join_where_literal(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where="first_order_week > '2021-07-12'",
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE DATE_TRUNC('WEEK', customers.first_order_date) > '2021-07-12' "
        "GROUP BY customers.region,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_multiple_join_having_dict(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12;"
    )
    assert query == correct


def test_query_multiple_join_having_literal(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having="total_item_revenue > -12",
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue) > -12;"
    )
    assert query == correct


def test_query_multiple_join_order_by_literal(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        order_by="total_item_revenue",
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat ORDER BY total_item_revenue ASC;"
    )
    assert query == correct


def test_query_multiple_join_all(connection):

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
        order_by=[{"field": "total_item_revenue", "sort": "desc"}],
    )

    correct = (
        "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region<>'West' "
        "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12 "
        "ORDER BY total_item_revenue DESC;"
    )
    assert query == correct
