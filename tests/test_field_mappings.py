import datetime
import pytest

from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.model.definitions import Definitions


@pytest.mark.query
def test_mapping_date_only(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["date"])

    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date "
        "FROM analytics.order_line_items order_lines GROUP BY DATE_TRUNC('DAY', "
        "order_lines.order_date) ORDER BY order_lines_order_date ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_dimension_only(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["source"])

    correct = (
        "SELECT sessions.utm_source as sessions_utm_source FROM analytics.sessions "
        "sessions GROUP BY sessions.utm_source ORDER BY sessions_utm_source ASC;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("time_grain", ["date", "week", "month", "quarter", "year"])
def test_mapping_metric_mapped_date_and_filter(connection, time_grain):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=[time_grain],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )

    if time_grain == "date":
        date_part = "DATE_TRUNC('DAY', orders.order_date)"
    elif time_grain == "week":
        date_part = "DATE_TRUNC('WEEK', CAST(orders.order_date AS DATE))"
    elif time_grain == "month":
        date_part = "DATE_TRUNC('MONTH', orders.order_date)"
    elif time_grain == "quarter":
        date_part = "DATE_TRUNC('QUARTER', orders.order_date)"
    elif time_grain == "year":
        date_part = "DATE_TRUNC('YEAR', orders.order_date)"

    correct = (
        f"SELECT {date_part} as orders_order_{time_grain},"
        "COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "WHERE DATE_TRUNC('DAY', orders.order_date)>='2022-01-05T00:00:00' "
        f"GROUP BY {date_part} ORDER BY orders_number_of_orders DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_same_canon_date_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["line_item_aov", "gross_revenue"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )
    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date,"
        "(SUM(order_lines.revenue)) / (NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  "
        "IS NOT NULL THEN  orders.id  ELSE NULL END), 0)) as order_lines_line_item_aov,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id"
        "=orders.id WHERE DATE_TRUNC('DAY', order_lines.order_date)>='2022-01-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', order_lines.order_date) "
        "ORDER BY order_lines_line_item_aov DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_different_canon_date_merged_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_than",
                "value": datetime.datetime(2023, 3, 5, 0, 0),
            },
        ],
        verbose=True,
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders "
        "orders WHERE DATE_TRUNC('DAY', orders.order_date)>='2022-01-05T00:00:00' AND "
        "DATE_TRUNC('DAY', orders.order_date)<'2023-03-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_number_of_orders DESC) ,"
        f"{sessions_cte} AS (SELECT DATE_TRUNC('DAY', sessions.session_date) "
        "as sessions_session_date,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions WHERE DATE_TRUNC('DAY', sessions.session_date)"
        ">='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', sessions.session_date)<'2023-03-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC) SELECT {orders_cte}."
        f"orders_number_of_orders as orders_number_of_orders,{sessions_cte}."
        f"sessions_number_of_sessions as sessions_number_of_sessions,ifnull({orders_cte}."
        f"orders_order_date, {sessions_cte}.sessions_session_date) as orders_order_date,"
        f"ifnull({sessions_cte}.sessions_session_date, {orders_cte}.orders_order_date) "
        f"as sessions_session_date FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.orders_order_date={sessions_cte}.sessions_session_date;"
    )

    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_different_canon_date_joinable_mapped_date_dim_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "gross_revenue"],
        dimensions=["source", "date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_than",
                "value": datetime.datetime(2023, 3, 5, 0, 0),
            },
        ],
        verbose=True,
    )

    orders_cte = "orders_order__cte_subquery_1"
    order_lines_cte = "order_lines_order__cte_subquery_0"
    correct = (
        f"WITH {orders_cte} AS (SELECT orders.sub_channel as orders_sub_channel,"
        f"DATE_TRUNC('DAY', orders.order_date) as orders_order_date,COUNT(orders.id) as "
        f"orders_number_of_orders FROM analytics.orders orders WHERE DATE_TRUNC('DAY', "
        f"orders.order_date)>='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', orders.order_date)"
        f"<'2023-03-05T00:00:00' GROUP BY orders.sub_channel,DATE_TRUNC('DAY', orders.order_date) "
        f"ORDER BY orders_number_of_orders DESC) ,"
        f"{order_lines_cte} AS ("
        f"SELECT orders.sub_channel as orders_sub_channel,DATE_TRUNC('DAY', order_lines.order_date) "
        f"as order_lines_order_date,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        f"FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        f"ON order_lines.order_unique_id=orders.id WHERE DATE_TRUNC('DAY', "
        f"order_lines.order_date)>='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', "
        f"order_lines.order_date)<'2023-03-05T00:00:00' GROUP BY orders.sub_channel,"
        f"DATE_TRUNC('DAY', order_lines.order_date) ORDER BY order_lines_total_item_revenue DESC) "
        f"SELECT {order_lines_cte}.order_lines_total_item_revenue as "
        f"order_lines_total_item_revenue,{orders_cte}.orders_number_of_orders "
        f"as orders_number_of_orders,ifnull({order_lines_cte}.orders_sub_channel, "
        f"{orders_cte}.orders_sub_channel) as orders_sub_channel,"
        f"ifnull({order_lines_cte}.order_lines_order_date, {orders_cte}.orders_order_date) "
        f"as order_lines_order_date,ifnull({orders_cte}.orders_order_date, "
        f"{order_lines_cte}.order_lines_order_date) as orders_order_date "
        f"FROM {order_lines_cte} FULL OUTER JOIN {orders_cte} "
        f"ON {order_lines_cte}.orders_sub_channel={orders_cte}"
        f".orders_sub_channel and {order_lines_cte}.order_lines_order_date"
        f"={orders_cte}.orders_order_date;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.druid])
def test_mapping_mapped_metric_joined_dim(connection, query_type):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "average_customer_ltv"],
        dimensions=["channel"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        query_type=query_type,
        verbose=True,
    )

    if_null = "nvl" if query_type == Definitions.druid else "ifnull"
    orders_cte = "orders_order__cte_subquery_1"
    customers_cte = "customers_first_order__cte_subquery_0"

    # Druid doesn't support symmetric aggregation, and casts timestamps differently
    if query_type == Definitions.druid:
        avg_query = "AVG(customers.customer_ltv)"
        count_query = "COUNT(orders.id)"
        orders_date_ref = "CAST(orders.order_date AS TIMESTAMP)"
        customers_date_ref = "CAST(customers.first_order_date AS TIMESTAMP)"
        order_by_count = ""
        order_by_avg = ""
        semi = ""
    else:
        avg_query = (
            "(COALESCE(CAST((SUM(DISTINCT "
            f"(CAST(FLOOR(COALESCE(customers.customer_ltv, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
            f"+ (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)"
            f"::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(customers.customer_id), "
            f"'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            f"/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
            f"(customers.customer_ltv)  IS NOT NULL THEN  customers.customer_id  ELSE NULL END), 0))"
        )
        count_query = (
            "NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN  orders.id  " "ELSE NULL END), 0)"
        )
        orders_date_ref = "orders.order_date"
        customers_date_ref = "customers.first_order_date"
        order_by_count = " ORDER BY orders_number_of_orders DESC"
        order_by_avg = " ORDER BY customers_average_customer_ltv DESC"
        semi = ";"
    correct = (
        f"WITH {orders_cte} AS (SELECT order_lines.sales_channel as order_lines_channel,"
        f"{count_query} as orders_number_of_orders FROM analytics.order_line_items order_lines "
        f"LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE "
        f"DATE_TRUNC('DAY', {orders_date_ref})>='2022-01-05T00:00:00' GROUP BY "
        f"order_lines.sales_channel{order_by_count}) ,"
        f"{customers_cte} AS (SELECT "
        f"order_lines.sales_channel as order_lines_channel,{avg_query} as customers_average_customer_ltv "
        "FROM analytics.order_line_items order_lines "
        f"LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        f"WHERE DATE_TRUNC('DAY', {customers_date_ref})>='2022-01-05T00:00:00' "
        f"GROUP BY order_lines.sales_channel{order_by_avg}) "
        f"SELECT {customers_cte}.customers_average_customer_ltv "
        f"as customers_average_customer_ltv,{orders_cte}.orders_number_of_orders "
        f"as orders_number_of_orders,{if_null}({customers_cte}."
        f"order_lines_channel, {orders_cte}.order_lines_channel) as order_lines_channel FROM "
        f"{customers_cte} FULL OUTER JOIN "
        f"{orders_cte} ON {customers_cte}"
        f".order_lines_channel={orders_cte}.order_lines_channel{semi}"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_mapped_metric_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["gross_revenue"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
        ],
        verbose=True,
    )
    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id WHERE DATE_TRUNC('DAY', "
        "order_lines.order_date)>='2022-01-05T00:00:00' AND orders.new_vs_repeat='New' "
        "GROUP BY DATE_TRUNC('DAY', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_metric_mapped_dim(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["source"],
        where=[
            {
                "field": "order_lines.order_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0) as orders_number_of_orders "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id WHERE order_lines.order_date"
        ">='2022-01-05T00:00:00' GROUP BY orders.sub_channel "
        "ORDER BY orders_number_of_orders DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_dim(connection):
    query = connection.get_sql_query(
        metrics=["gross_revenue"],
        dimensions=["source"],
        where=[{"field": "source", "expression": "equal_to", "value": "google"}],
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "WHERE orders.sub_channel='google' GROUP BY orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_non_mapped_dim(connection):
    query = connection.get_sql_query(metrics=["gross_revenue"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_dim_having(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["source"],
        having=[{"field": "gross_revenue", "expression": "greater_than", "value": 200}],
        order_by=[{"field": "gross_revenue", "sort": "asc"}, {"field": "source", "sort": "desc"}],
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0) "
        "as orders_number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel HAVING SUM(order_lines.revenue)>200 "
        "ORDER BY order_lines_total_item_revenue ASC,orders_sub_channel DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_merged_results(connection):
    query = connection.get_sql_query(metrics=["gross_revenue", "number_of_sessions"], dimensions=["source"])

    order_lines_cte = "order_lines_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"

    sessions_source = f"ifnull({sessions_cte}.sessions_utm_source, {order_lines_cte}.orders_sub_channel)"
    order_lines_source = f"ifnull({order_lines_cte}.orders_sub_channel, {sessions_cte}.sessions_utm_source)"
    correct = (
        f"WITH {order_lines_cte} AS (SELECT orders.sub_channel as orders_sub_channel,"
        f"SUM(order_lines.revenue) as order_lines_total_item_revenue FROM analytics.order_line_items "
        f"order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        f"GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{sessions_cte} AS (SELECT sessions.utm_source as sessions_utm_source,"
        f"COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        f"GROUP BY sessions.utm_source ORDER BY sessions_number_of_sessions DESC) "
        f"SELECT {order_lines_cte}.order_lines_total_item_revenue as "
        f"order_lines_total_item_revenue,{sessions_cte}.sessions_number_of_sessions "
        f"as sessions_number_of_sessions,{order_lines_source} as "
        f"orders_sub_channel,{sessions_source} as "
        f"sessions_utm_source FROM {order_lines_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {order_lines_cte}.orders_sub_channel"
        f"={sessions_cte}.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_incorrect_error_message_on_mapped_filter(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"],
            dimensions=["source"],
            where=[
                {"field": "source", "expression": "equal_to", "value": "google"},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
            ],
        )

    correct_error = (
        f"The field number_of_sessions could not be either joined into the query or mapped "
        "and merged into the query as a merged result. \n\nCheck that you specify joins to join it "
        "in, or specify a mapping for a query with two tables that cannot be merged"
    )
    assert exc_info.value
    assert str(exc_info.value) == correct_error

    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"],
            dimensions=["source", "sessions.session_id"],
            where=[
                {"field": "source", "expression": "equal_to", "value": "google"},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
            ],
        )

    correct_error = (
        f"The query could not be either joined or mapped and merged into a valid query with the fields:"
        "\n\nnumber_of_orders, number_of_sessions, sessions.session_id, new_vs_repeat, source\n\n"
        "Check that those fields can be joined together or are mapped so they can be merged across tables"
    )
    assert exc_info.value
    assert str(exc_info.value) == correct_error


@pytest.mark.query
def test_dim_only_joinable_date_chooses_right_mapping_date(connection):
    query = connection.get_sql_query(
        metrics=[],
        dimensions=["orders.customer_id", "orders.account_id", "orders.sub_channel", "orders.campaign"],
        where=[
            {"field": "date", "expression": "greater_or_equal_than", "value": "2023-05-05"},
            {"field": "date", "expression": "less_or_equal_than", "value": "2023-08-02"},
        ],
    )

    correct = (
        "SELECT orders.customer_id as orders_customer_id,orders.account_id as orders_account_id,"
        "orders.sub_channel as orders_sub_channel,orders.campaign as orders_campaign "
        "FROM analytics.orders orders WHERE DATE_TRUNC('DAY', orders.order_date)>='2023-05-05' "
        "AND DATE_TRUNC('DAY', orders.order_date)<='2023-08-02' GROUP BY orders.customer_id,"
        "orders.account_id,orders.sub_channel,orders.campaign ORDER BY orders_customer_id ASC;"
    )
    assert query == correct


@pytest.mark.queryy
def test_mapping_defer_to_metric_canon_date_not_dim_only(connection):
    query = connection.get_sql_query(
        metrics=["number_of_clicks", "unique_users_form_submissions"],
        dimensions=["submitted_form.sent_at_date", "submitted_form.context_os"],
        where=[{"field": "date", "expression": "greater_or_equal_than", "value": "2023-05-05"}],
    )

    cte_1 = "clicked_on_page_session__cte_subquery_0"
    cte_2 = "submitted_form_sent_at__cte_subquery_1"
    correct = (
        "WITH clicked_on_page_session__cte_subquery_0 AS (SELECT DATE_TRUNC('DAY', "
        "clicked_on_page.session_date) as clicked_on_page_session_date,"
        "clicked_on_page.context_os as clicked_on_page_context_os,"
        "COUNT(clicked_on_page.id) as clicked_on_page_number_of_clicks "
        "FROM analytics.clicked_on_page clicked_on_page WHERE DATE_TRUNC('DAY', "
        "clicked_on_page.session_date)>='2023-05-05' GROUP BY DATE_TRUNC('DAY', "
        "clicked_on_page.session_date),clicked_on_page.context_os ORDER BY "
        "clicked_on_page_number_of_clicks DESC) ,"
        "submitted_form_sent_at__cte_subquery_1 AS (SELECT DATE_TRUNC('DAY', "
        "submitted_form.session_date) as submitted_form_sent_at_date,"
        "submitted_form.context_os as submitted_form_context_os,"
        "COUNT(DISTINCT(submitted_form.customer_id)) as submitted_form_unique_users_form_submissions "
        "FROM analytics.submitted_form submitted_form WHERE DATE_TRUNC('DAY', "
        "submitted_form.session_date)>='2023-05-05' GROUP BY DATE_TRUNC('DAY', "
        "submitted_form.session_date),submitted_form.context_os ORDER BY "
        "submitted_form_unique_users_form_submissions DESC) SELECT "
        f"{cte_1}.clicked_on_page_number_of_clicks "
        f"as clicked_on_page_number_of_clicks,{cte_2}"
        f".submitted_form_unique_users_form_submissions as "
        f"submitted_form_unique_users_form_submissions,"
        f"ifnull({cte_1}.clicked_on_page_session_date, {cte_2}.submitted_form_sent_at_date) "
        f"as clicked_on_page_session_date,ifnull({cte_1}.clicked_on_page_context_os, "
        f"{cte_2}.submitted_form_context_os) as clicked_on_page_context_os,"
        f"ifnull({cte_2}.submitted_form_sent_at_date, {cte_1}.clicked_on_page_session_date) "
        f"as submitted_form_sent_at_date,ifnull({cte_2}.submitted_form_context_os, "
        f"{cte_1}.clicked_on_page_context_os) as submitted_form_context_os "
        f"FROM {cte_1} FULL OUTER JOIN {cte_2} ON {cte_1}."
        f"clicked_on_page_session_date={cte_2}.submitted_form_sent_at_date "
        f"and {cte_1}.clicked_on_page_context_os={cte_2}.submitted_form_context_os;"
    )
    assert query == correct
