import pytest

from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.exceptions import QueryError


@pytest.mark.query
def test_orders_funnel_query_missing_arg(connection):
    funnel = {
        "steps": [
            [{"field": "channel", "expression": "equal_to", "value": "Paid"}],
            [{"field": "channel", "expression": "isin", "value": ["Organic", "Email"]}],
        ],
    }
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(metrics=["number_of_orders"], funnel=funnel)

    assert exc_info.value


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift])
def test_orders_funnel_query(connection, query_type):
    funnel = {
        "steps": [
            [{"field": "channel", "expression": "equal_to", "value": "Paid"}],
            [{"field": "channel", "expression": "isin", "value": ["Organic", "Email"]}],
        ],
        "within": {"value": 3, "unit": "days"},
    }
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        funnel=funnel,
        where=[{"field": "region", "expression": "equal_to", "value": "West"}],
        query_type=query_type,
    )

    if query_type == Definitions.bigquery:
        date_diff = "DATE_DIFF(CAST(base.orders_order_raw as DATE), CAST(step_1.step_1_time as DATE), DAY)"
    else:
        date_diff = "DATEDIFF('DAY', step_1.step_1_time, base.orders_order_raw)"

    correct = (
        "WITH base AS (SELECT order_lines.sales_channel as order_lines_channel,customers.customer_id "
        "as customers_customer_id,orders.id as orders_order_id,orders.order_date as orders_order_raw,"
        "orders.id as orders_number_of_orders "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers "
        "ON order_lines.customer_id=customers.customer_id WHERE customers.region='West') ,step_1 AS ("
        "SELECT *,orders_order_raw as step_1_time FROM base WHERE base.order_lines_channel='Paid') ,"
        "step_2 AS (SELECT base.*,step_1.step_1_time as step_1_time FROM base "
        "JOIN step_1 ON base.customers_customer_id=step_1.customers_customer_id "
        "and step_1.step_1_time<=base.orders_order_raw "
        f"WHERE base.order_lines_channel IN ('Organic','Email') AND {date_diff} <= 3) ,result_cte AS ("
        "(SELECT 'Step 1' as step,1 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), "
        "0) as orders_number_of_orders FROM step_1) "
        "UNION ALL "
        "(SELECT 'Step 2' as step,2 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), "
        "0) as orders_number_of_orders FROM step_2)) "
        "SELECT * FROM result_cte;"
    )
    assert query == correct


# Test with additional metrics
@pytest.mark.query
def test_orders_funnel_query_metrics(connection):
    funnel = {
        "view_name": "orders",
        "steps": [
            [{"field": "channel", "expression": "equal_to", "value": "Paid"}],
            [{"field": "channel", "expression": "equal_to", "value": "Organic"}],
            [{"field": "channel", "expression": "isin", "value": ["Organic", "Email"]}],
        ],
        "within": {"value": 20, "unit": "hours"},
    }
    query = connection.get_sql_query(metrics=["number_of_orders", "total_item_revenue"], funnel=funnel)

    correct = (
        "WITH base AS (SELECT order_lines.sales_channel as order_lines_channel,customers.customer_id "
        "as customers_customer_id,order_lines.order_line_id as order_lines_order_line_id,orders.id as "
        "orders_order_id,orders.order_date as orders_order_raw,orders.id as orders_number_of_orders,"
        "order_lines.revenue as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id) ,"
        "step_1 AS (SELECT *,orders_order_raw as step_1_time FROM base WHERE base.order_lines_channel='Paid')"
        " ,step_2 AS ("
        "SELECT base.*,step_1.step_1_time as step_1_time FROM base JOIN step_1 ON base.customers_customer_id"
        "=step_1.customers_customer_id and step_1.step_1_time<=base.orders_order_raw "
        "WHERE base.order_lines_channel='Organic' AND DATEDIFF('HOUR', step_1.step_1_time, "
        "base.orders_order_raw) <= 20) ,step_3 AS (SELECT base.*,step_2.step_1_time as step_1_time "
        "FROM base JOIN step_2 ON base.customers_customer_id=step_2.customers_customer_id and "
        "step_2.step_1_time<=base.orders_order_raw WHERE base.order_lines_channel IN ('Organic','Email') "
        "AND DATEDIFF('HOUR', step_2.step_1_time, base.orders_order_raw) <= 20) ,result_cte AS ((SELECT "
        "'Step 1' as step,1 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  (orders_number_of_orders)  "
        "IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as orders_number_of_orders,"
        "SUM(order_lines_total_item_revenue) as order_lines_total_item_revenue FROM step_1) UNION ALL "
        "(SELECT 'Step 2' as step,2 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  (orders_number_of_orders)"
        "  IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as orders_number_of_orders,"
        "SUM(order_lines_total_item_revenue) as order_lines_total_item_revenue FROM step_2) UNION ALL (SELECT"
        " 'Step 3' as step,3 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  (orders_number_of_orders)  IS NOT"
        " NULL THEN  orders_order_id  ELSE NULL END), 0) as orders_number_of_orders,"
        "SUM(order_lines_total_item_revenue) as order_lines_total_item_revenue FROM step_3)) "
        "SELECT * FROM result_cte;"
    )
    assert query == correct


# Test with additional dimensions
@pytest.mark.query
def test_orders_funnel_query_dimensions(connection):
    funnel = {
        "view_name": "orders",
        "steps": [
            [{"field": "channel", "expression": "equal_to", "value": "Paid"}],
            [{"field": "region", "expression": "not_equal_to", "value": "West"}],
            [{"field": "channel", "expression": "equal_to", "value": "Organic"}],
        ],
        "within": {"value": 2, "unit": "weeks"},
    }
    query = connection.get_sql_query(
        metrics=["number_of_orders"], dimensions=["orders.order_month", "region"], funnel=funnel
    )

    correct = (
        "WITH base AS (SELECT order_lines.sales_channel as order_lines_channel,customers.customer_id "
        "as customers_customer_id,orders.id as orders_order_id,DATE_TRUNC('MONTH', orders.order_date) "
        "as orders_order_month,orders.order_date as orders_order_raw,customers.region as "
        "customers_region,orders.id as orders_number_of_orders FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id) "
        ",step_1 AS (SELECT *,orders_order_raw as step_1_time FROM base WHERE base.order_lines_channel='Paid'"
        ") ,step_2 AS (SELECT base.*,step_1.step_1_time as step_1_time FROM base JOIN step_1 "
        "ON base.customers_customer_id=step_1.customers_customer_id and step_1.step_1_time"
        "<=base.orders_order_raw WHERE base.customers_region<>'West' AND DATEDIFF('WEEK', "
        "step_1.step_1_time, base.orders_order_raw) <= 2) ,step_3 AS (SELECT base.*,step_2.step_1_time "
        "as step_1_time FROM base JOIN step_2 ON base.customers_customer_id=step_2.customers_customer_id "
        "and step_2.step_1_time<=base.orders_order_raw WHERE base.order_lines_channel='Organic' AND "
        "DATEDIFF('WEEK', step_2.step_1_time, base.orders_order_raw) <= 2) ,result_cte AS ((SELECT "
        "'Step 1' as step,1 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  (orders_number_of_orders)  "
        "IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as orders_number_of_orders,orders_order_month "
        "as orders_order_month,customers_region as customers_region FROM step_1 GROUP BY orders_order_month"
        ",customers_region) UNION ALL (SELECT 'Step 2' as step,2 as step_order,NULLIF(COUNT(DISTINCT CASE "
        "WHEN  (orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) "
        "as orders_number_of_orders,orders_order_month as orders_order_month,customers_region as "
        "customers_region FROM step_2 GROUP BY orders_order_month,customers_region) UNION ALL (SELECT"
        " 'Step 3' as step,3 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  (orders_number_of_orders)  "
        "IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as orders_number_of_orders,"
        "orders_order_month as orders_order_month,customers_region as customers_region FROM "
        "step_3 GROUP BY orders_order_month,customers_region)) SELECT * FROM result_cte;"
    )
    assert query == correct


@pytest.mark.query
def test_orders_funnel_query_complex_conditions(connection):
    funnel = {
        "view_name": "orders",
        "steps": [
            [
                {"field": "channel", "expression": "equal_to", "value": "Paid"},
                {"field": "region", "expression": "not_equal_to", "value": "West"},
            ],
            [
                {"field": "channel", "expression": "equal_to", "value": "Organic"},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "Repeat"},
            ],
        ],
        "within": {"value": 2, "unit": "weeks"},
    }
    query = connection.get_sql_query(
        metrics=["number_of_customers"],
        funnel=funnel,
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": 320}],
    )

    correct = (
        "WITH base AS (SELECT order_lines.sales_channel as order_lines_channel,customers.customer_id "
        "as customers_customer_id,orders.new_vs_repeat as orders_new_vs_repeat,order_lines.order_line_id "
        "as order_lines_order_line_id,orders.order_date as orders_order_raw,customers.region as "
        "customers_region,customers.customer_id as customers_number_of_customers,order_lines.revenue "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN "
        "analytics.orders orders ON order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers "
        "customers ON order_lines.customer_id=customers.customer_id) ,step_1 AS (SELECT *,orders_order_raw "
        "as step_1_time FROM base WHERE base.order_lines_channel='Paid' AND base.customers_region<>'West')"
        " ,step_2 AS (SELECT base.*,step_1.step_1_time as step_1_time FROM base JOIN step_1 ON "
        "base.customers_customer_id=step_1.customers_customer_id and step_1.step_1_time"
        "<=base.orders_order_raw WHERE base.order_lines_channel='Organic' AND base.orders_new_vs_repeat"
        "='Repeat' AND DATEDIFF('WEEK', step_1.step_1_time, base.orders_order_raw) <= 2) ,"
        "result_cte AS ((SELECT 'Step 1' as step,1 as step_order,"
        "COUNT(DISTINCT(customers_number_of_customers)) as customers_number_of_customers,"
        "SUM(order_lines_total_item_revenue) as order_lines_total_item_revenue FROM step_1) "
        "UNION ALL (SELECT 'Step 2' as step,2 as step_order,COUNT(DISTINCT(customers_number_of_customers))"
        " as customers_number_of_customers,SUM(order_lines_total_item_revenue) as "
        "order_lines_total_item_revenue FROM step_2)) "
        "SELECT * FROM result_cte WHERE order_lines_total_item_revenue>320;"
    )
    assert query == correct