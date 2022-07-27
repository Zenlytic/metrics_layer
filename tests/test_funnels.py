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
        "WITH base AS (SELECT orders.order_date as orders_order_raw,"
        "customers.customer_id as customers_customer_id,order_lines.sales_channel as order_lines_channel,"
        "orders.id as orders_order_id,orders.id as orders_number_of_orders "
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
# Test with additional dimensions
# Test with additional event conditions and alias for conditions
