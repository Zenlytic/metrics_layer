import pytest


@pytest.mark.query
@pytest.mark.parametrize("metric_suffix", ["end_of_month", "beginning_of_month"])
def test_mrr_non_additive_dimension_no_group_by_max(connection, metric_suffix):
    query = connection.get_sql_query(metrics=[f"mrr_{metric_suffix}"])

    func = "MAX" if metric_suffix == "end_of_month" else "MIN"
    correct = (
        f"WITH cte_mrr_{metric_suffix}_record_raw AS (SELECT {func}(mrr.record_date) as mrr_{func.lower()}_record_raw "  # noqa
        f"FROM analytics.mrr_by_customer mrr ORDER BY mrr_{func.lower()}_record_raw DESC) "
        f"SELECT SUM(case when mrr.record_date=cte_mrr_{metric_suffix}_record_raw.mrr_{func.lower()}_record_raw "  # noqa
        f"then mrr.mrr end) as mrr_mrr_{metric_suffix} FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_{metric_suffix}_record_raw ON 1=1 ORDER BY mrr_mrr_{metric_suffix} DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_no_group_by_multi_cte(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month", "mrr_beginning_of_month", "mrr_end_of_month_by_account"]
    )

    correct = (
        "WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as"
        " mrr_account_id,MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date FROM"
        " analytics.mrr_by_customer mrr GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC)"
        " ,cte_mrr_end_of_month_record_raw AS (SELECT MAX(mrr.record_date) as mrr_max_record_raw FROM"
        " analytics.mrr_by_customer mrr ORDER BY mrr_max_record_raw DESC)"
        " ,cte_mrr_beginning_of_month_record_raw AS (SELECT MIN(mrr.record_date) as mrr_min_record_raw FROM"
        " analytics.mrr_by_customer mrr ORDER BY mrr_min_record_raw DESC) SELECT SUM(case when"
        " mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw then mrr.mrr end) as"
        " mrr_mrr_end_of_month,SUM(case when"
        " mrr.record_date=cte_mrr_beginning_of_month_record_raw.mrr_min_record_raw then mrr.mrr end) as"
        " mrr_mrr_beginning_of_month,SUM(case when DATE_TRUNC('DAY',"
        " mrr.record_date)=cte_mrr_end_of_month_by_account_record_date.mrr_max_record_date and"
        " mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id then mrr.mrr end) as"
        " mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr JOIN"
        " cte_mrr_end_of_month_by_account_record_date ON"
        " mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id JOIN"
        " cte_mrr_end_of_month_record_raw ON 1=1 JOIN cte_mrr_beginning_of_month_record_raw ON 1=1 ORDER BY"
        " mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.queryyy
def test_mrr_non_additive_dimension_no_group_by_composed(connection):
    query = connection.get_sql_query(metrics=[f"mrr_change_per_billed_account"])

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT MAX(mrr.record_date) as mrr_max_record_raw FROM"
        " analytics.mrr_by_customer mrr ORDER BY mrr_max_record_raw DESC)"
        " ,cte_mrr_beginning_of_month_record_raw AS (SELECT MIN(mrr.record_date) as mrr_min_record_raw FROM"
        " analytics.mrr_by_customer mrr ORDER BY mrr_min_record_raw DESC) SELECT ((SUM(case when"
        " mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw then mrr.mrr end)) - (SUM(case"
        " when mrr.record_date=cte_mrr_beginning_of_month_record_raw.mrr_min_record_raw then mrr.mrr end))) /"
        " (COUNT(mrr.parent_account_id)) as mrr_mrr_change_per_billed_account FROM analytics.mrr_by_customer"
        " mrr JOIN cte_mrr_end_of_month_record_raw ON 1=1 JOIN cte_mrr_beginning_of_month_record_raw ON 1=1"
        " ORDER BY mrr_mrr_change_per_billed_account DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_no_group_by_with_where(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month"],
        where=[
            {"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"},
            {"field": "mrr.record_date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    correct = (
        f"WITH cte_mrr_end_of_month_record_raw AS (SELECT MAX(mrr.record_date) as mrr_max_record_raw "
        f"FROM analytics.mrr_by_customer mrr WHERE mrr.plan_name='Enterprise' "
        "AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' ORDER BY mrr_max_record_raw DESC) "
        f"SELECT SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "  # noqa
        f"then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_record_raw ON 1=1 WHERE mrr.plan_name='Enterprise' "
        "AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_no_group_by_with_window_grouping(connection):
    query = connection.get_sql_query(metrics=[f"mrr_end_of_month_by_account"])

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as mrr_account_id,"
        f"MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC) "
        f"SELECT SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_record_date"
        f".mrr_max_record_date and mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_by_account_record_date ON mrr.account_id"
        f"=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"ORDER BY mrr_mrr_end_of_month_by_account DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_time_group_by(connection):
    query = connection.get_sql_query(metrics=["mrr_end_of_month"], dimensions=["mrr.record_week"])

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "as mrr_record_week,MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "ORDER BY mrr_max_record_raw DESC) "
        "SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"
        "=cte_mrr_end_of_month_record_raw.mrr_record_week GROUP BY DATE_TRUNC('WEEK', "
        "CAST(mrr.record_date AS DATE)) ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_time_group_by_with_where(connection):
    query = connection.get_sql_query(
        metrics=["mrr_end_of_month"],
        dimensions=["mrr.record_week"],
        where=[
            {"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"},
            {"field": "mrr.record_date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "as mrr_record_week,MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr WHERE mrr.plan_name='Enterprise' "
        "AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' GROUP BY DATE_TRUNC('WEEK', "
        "CAST(mrr.record_date AS DATE)) ORDER BY mrr_max_record_raw DESC) "
        "SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"
        "=cte_mrr_end_of_month_record_raw.mrr_record_week "
        "WHERE mrr.plan_name='Enterprise' AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' "
        "GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_time_group_by_with_window_grouping(connection):
    query = connection.get_sql_query(metrics=[f"mrr_end_of_month_by_account"], dimensions=["mrr.record_week"])

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as mrr_account_id,"
        f"DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        f"MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id,DATE_TRUNC('WEEK', CAST(mrr.record_date "
        f"AS DATE)) ORDER BY mrr_max_record_date DESC) "
        f"SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        f"SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_record_date"
        f".mrr_max_record_date "
        f"and mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_by_account_record_date ON mrr.account_id"
        f"=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"and DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"
        f"=cte_mrr_end_of_month_by_account_record_date.mrr_record_week "
        f"GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        f"ORDER BY mrr_mrr_end_of_month_by_account DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by(connection):
    query = connection.get_sql_query(metrics=["mrr_end_of_month"], dimensions=["mrr.plan_name"])

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT mrr.plan_name as mrr_plan_name,"
        "MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr GROUP BY mrr.plan_name ORDER BY mrr_max_record_raw DESC) "
        "SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON mrr.plan_name=cte_mrr_end_of_month_record_raw.mrr_plan_name "
        "GROUP BY mrr.plan_name ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by_with_where(connection):
    query = connection.get_sql_query(
        metrics=["mrr_end_of_month"],
        dimensions=["mrr.plan_name"],
        where=[
            {"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"},
            {"field": "mrr.record_date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT mrr.plan_name as mrr_plan_name,"
        "MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr WHERE mrr.plan_name='Enterprise' "
        "AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' GROUP BY mrr.plan_name ORDER BY mrr_max_record_raw DESC) "  # noqa
        "SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON mrr.plan_name=cte_mrr_end_of_month_record_raw.mrr_plan_name "
        "WHERE mrr.plan_name='Enterprise' "
        "AND DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' "
        "GROUP BY mrr.plan_name ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by_with_having(connection):
    query = connection.get_sql_query(
        metrics=["mrr_end_of_month"],
        dimensions=["mrr.plan_name"],
        where=[{"field": "mrr.plan_name", "expression": "equal_to", "value": "Enterprise"}],
        having=[{"field": "mrr.mrr_end_of_month", "expression": "greater_than", "value": 1100}],
    )

    correct = (
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT mrr.plan_name as mrr_plan_name,"
        "MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr WHERE mrr.plan_name='Enterprise' "
        "GROUP BY mrr.plan_name ORDER BY mrr_max_record_raw DESC) "
        "SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON mrr.plan_name=cte_mrr_end_of_month_record_raw.mrr_plan_name "
        "WHERE mrr.plan_name='Enterprise' "
        "GROUP BY mrr.plan_name "
        "HAVING SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw"
        ".mrr_max_record_raw then mrr.mrr end)>1100 ORDER BY mrr_mrr_end_of_month DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by_with_window_grouping(connection):
    query = connection.get_sql_query(metrics=[f"mrr_end_of_month_by_account"], dimensions=["mrr.plan_name"])

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as mrr_account_id,"
        "mrr.plan_name as mrr_plan_name,MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id,mrr.plan_name ORDER BY mrr_max_record_date DESC) "  # noqa
        f"SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_record_date"
        ".mrr_max_record_date "
        "and mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_by_account_record_date ON mrr.account_id"
        "=cte_mrr_end_of_month_by_account_record_date.mrr_account_id and mrr.plan_name"
        "=cte_mrr_end_of_month_by_account_record_date.mrr_plan_name "
        "GROUP BY mrr.plan_name "
        "ORDER BY mrr_mrr_end_of_month_by_account DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_group_by_equal_to_window_grouping(connection):
    query = connection.get_sql_query(metrics=[f"mrr_end_of_month_by_account"], dimensions=["mrr.account_id"])

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as mrr_account_id,"
        f"MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC) "
        f"SELECT mrr.account_id as mrr_account_id,"
        f"SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_record_date"
        f".mrr_max_record_date "
        f"and mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_by_account_record_date ON mrr.account_id"
        f"=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"GROUP BY mrr.account_id "
        f"ORDER BY mrr_mrr_end_of_month_by_account DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_merged_results_no_group_by_where(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month", "total_item_revenue"],
        where=[{"field": "date", "expression": "greater_than", "value": "2022-04-03"}],
    )

    correct = (
        "WITH mrr_record__cte_subquery_0 AS (WITH cte_mrr_end_of_month_record_raw AS ("
        "SELECT MAX(mrr.record_date) as mrr_max_record_raw FROM analytics.mrr_by_customer mrr "
        "WHERE DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' ORDER BY mrr_max_record_raw DESC) "
        "SELECT SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw."
        "mrr_max_record_raw then mrr.mrr end) as mrr_mrr_end_of_month "
        "FROM analytics.mrr_by_customer mrr JOIN cte_mrr_end_of_month_record_raw ON 1=1 "
        "WHERE DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' ORDER BY mrr_mrr_end_of_month DESC"
        ") ,order_lines_order__cte_subquery_1 AS (SELECT SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "WHERE DATE_TRUNC('DAY', order_lines.order_date)>'2022-04-03' ORDER BY "
        "order_lines_total_item_revenue DESC) SELECT mrr_record__cte_subquery_0.mrr_mrr_end_of_month "
        "as mrr_mrr_end_of_month,order_lines_order__cte_subquery_1.order_lines_total_item_revenue "
        "as order_lines_total_item_revenue FROM mrr_record__cte_subquery_0 FULL OUTER JOIN "
        "order_lines_order__cte_subquery_1 ON 1=1;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_merged_results_no_group_by_window_grouping(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month_by_account", "total_item_revenue"],
        where=[{"field": "date", "expression": "greater_than", "value": "2022-04-03"}],
    )

    correct = (
        "WITH mrr_record__cte_subquery_0 AS ("
        f"WITH cte_mrr_end_of_month_by_account_record_date AS (SELECT mrr.account_id as mrr_account_id,"
        "MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr WHERE DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' "
        "GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC) "
        f"SELECT SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_record_date"
        ".mrr_max_record_date "
        "and mrr.account_id=cte_mrr_end_of_month_by_account_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account FROM analytics.mrr_by_customer mrr "
        f"JOIN cte_mrr_end_of_month_by_account_record_date ON mrr.account_id"
        "=cte_mrr_end_of_month_by_account_record_date.mrr_account_id WHERE DATE_TRUNC('DAY', mrr.record_date)>'2022-04-03' "  # noqa
        "ORDER BY mrr_mrr_end_of_month_by_account DESC"
        ") ,order_lines_order__cte_subquery_1 AS (SELECT SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "WHERE DATE_TRUNC('DAY', order_lines.order_date)>'2022-04-03' ORDER BY "
        "order_lines_total_item_revenue DESC) SELECT mrr_record__cte_subquery_0.mrr_mrr_end_of_month_by_account "  # noqa
        "as mrr_mrr_end_of_month_by_account,order_lines_order__cte_subquery_1.order_lines_total_item_revenue "
        "as order_lines_total_item_revenue FROM mrr_record__cte_subquery_0 FULL OUTER JOIN "
        "order_lines_order__cte_subquery_1 ON 1=1;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_merged_results_time_group_by(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month", "total_item_revenue"],
        dimensions=["mrr.record_week"],
    )

    correct = (
        "WITH mrr_record__cte_subquery_0 AS ("
        "WITH cte_mrr_end_of_month_record_raw AS (SELECT DATE_TRUNC('WEEK', "
        "CAST(mrr.record_date AS DATE)) as mrr_record_week,MAX(mrr.record_date) as mrr_max_record_raw "
        "FROM analytics.mrr_by_customer mrr GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "ORDER BY mrr_max_record_raw DESC) "
        "SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "SUM(case when mrr.record_date=cte_mrr_end_of_month_record_raw.mrr_max_record_raw "
        "then mrr.mrr end) as mrr_mrr_end_of_month FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_end_of_month_record_raw ON DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"
        "=cte_mrr_end_of_month_record_raw.mrr_record_week "
        "GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) ORDER BY mrr_mrr_end_of_month DESC"
        ") ,order_lines_order__cte_subquery_1 AS (SELECT DATE_TRUNC('WEEK', "
        "CAST(order_lines.order_date AS DATE)) as order_lines_order_week,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM analytics.order_line_items "
        "order_lines GROUP BY DATE_TRUNC('WEEK', CAST(order_lines.order_date AS DATE)) "
        "ORDER BY order_lines_total_item_revenue DESC) SELECT mrr_record__cte_subquery_0."
        "mrr_mrr_end_of_month as mrr_mrr_end_of_month,order_lines_order__cte_subquery_1."
        "order_lines_total_item_revenue as order_lines_total_item_revenue,ifnull("
        "mrr_record__cte_subquery_0.mrr_record_week, order_lines_order__cte_subquery_1"
        ".order_lines_order_week) as mrr_record_week,ifnull(order_lines_order__cte_subquery_1"
        ".order_lines_order_week, mrr_record__cte_subquery_0.mrr_record_week) as order_lines_order_week "
        "FROM mrr_record__cte_subquery_0 FULL OUTER JOIN order_lines_order__cte_subquery_1 "
        "ON mrr_record__cte_subquery_0.mrr_record_week=order_lines_order__cte_subquery_1.order_lines_order_week;"  # noqa
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_time_group_by_ignore_dimensions(connection):
    query = connection.get_sql_query(
        metrics=["mrr_beginning_of_month_no_group_by"], dimensions=["mrr.record_week"]
    )

    correct = (
        "WITH cte_mrr_beginning_of_month_no_group_by_record_raw AS (SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "  # noqa
        "as mrr_record_week,MIN(mrr.record_date) as mrr_min_record_raw "
        "FROM analytics.mrr_by_customer mrr GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "ORDER BY mrr_min_record_raw DESC) "
        "SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "SUM(case when mrr.record_date=cte_mrr_beginning_of_month_no_group_by_record_raw.mrr_min_record_raw "
        "then mrr.mrr end) as mrr_mrr_beginning_of_month_no_group_by FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_beginning_of_month_no_group_by_record_raw ON DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"  # noqa
        "=cte_mrr_beginning_of_month_no_group_by_record_raw.mrr_record_week GROUP BY DATE_TRUNC('WEEK', "
        "CAST(mrr.record_date AS DATE)) ORDER BY mrr_mrr_beginning_of_month_no_group_by DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_time_group_by_with_window_grouping_ignore_dimensions(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month_by_account_no_group_by"], dimensions=["mrr.record_week"]
    )

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_no_group_by_record_date AS (SELECT mrr.account_id as mrr_account_id,"  # noqa
        "DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id,DATE_TRUNC('WEEK', CAST(mrr.record_date "
        "AS DATE)) ORDER BY mrr_max_record_date DESC) "
        f"SELECT DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) as mrr_record_week,"
        "SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_no_group_by_record_date"  # noqa
        ".mrr_max_record_date "
        "and mrr.account_id=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account_no_group_by FROM analytics.mrr_by_customer mrr "  # noqa
        f"JOIN cte_mrr_end_of_month_by_account_no_group_by_record_date ON mrr.account_id"
        "=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        "and DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE))"
        "=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_record_week "
        "GROUP BY DATE_TRUNC('WEEK', CAST(mrr.record_date AS DATE)) "
        "ORDER BY mrr_mrr_end_of_month_by_account_no_group_by DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by_ignore_dimensions(connection):
    query = connection.get_sql_query(
        metrics=["mrr_beginning_of_month_no_group_by"], dimensions=["mrr.plan_name"]
    )

    correct = (
        "WITH cte_mrr_beginning_of_month_no_group_by_record_raw AS (SELECT "
        "MIN(mrr.record_date) as mrr_min_record_raw "
        "FROM analytics.mrr_by_customer mrr ORDER BY mrr_min_record_raw DESC) "
        "SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when mrr.record_date=cte_mrr_beginning_of_month_no_group_by_record_raw.mrr_min_record_raw "
        "then mrr.mrr end) as mrr_mrr_beginning_of_month_no_group_by FROM analytics.mrr_by_customer mrr "
        "JOIN cte_mrr_beginning_of_month_no_group_by_record_raw ON 1=1 "
        "GROUP BY mrr.plan_name ORDER BY mrr_mrr_beginning_of_month_no_group_by DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_alt_group_by_with_window_grouping_ignore_dimensions(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month_by_account_no_group_by"], dimensions=["mrr.plan_name"]
    )

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_no_group_by_record_date AS (SELECT mrr.account_id as mrr_account_id,"  # noqa
        "MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC) "  # noqa
        f"SELECT mrr.plan_name as mrr_plan_name,"
        "SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_no_group_by_record_date"  # noqa
        ".mrr_max_record_date "
        "and mrr.account_id=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account_no_group_by FROM analytics.mrr_by_customer mrr "  # noqa
        f"JOIN cte_mrr_end_of_month_by_account_no_group_by_record_date ON mrr.account_id"
        "=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        "GROUP BY mrr.plan_name ORDER BY mrr_mrr_end_of_month_by_account_no_group_by DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mrr_non_additive_dimension_group_by_equal_to_window_grouping_ignore_dimensions(connection):
    query = connection.get_sql_query(
        metrics=[f"mrr_end_of_month_by_account_no_group_by"], dimensions=["mrr.account_id"]
    )

    correct = (
        f"WITH cte_mrr_end_of_month_by_account_no_group_by_record_date AS (SELECT mrr.account_id as mrr_account_id,"  # noqa
        "MAX(DATE_TRUNC('DAY', mrr.record_date)) as mrr_max_record_date "
        f"FROM analytics.mrr_by_customer mrr GROUP BY mrr.account_id ORDER BY mrr_max_record_date DESC) "
        f"SELECT mrr.account_id as mrr_account_id,"
        "SUM(case when DATE_TRUNC('DAY', mrr.record_date)=cte_mrr_end_of_month_by_account_no_group_by_record_date"  # noqa
        ".mrr_max_record_date "
        "and mrr.account_id=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        f"then mrr.mrr end) as mrr_mrr_end_of_month_by_account_no_group_by FROM analytics.mrr_by_customer mrr "  # noqa
        f"JOIN cte_mrr_end_of_month_by_account_no_group_by_record_date ON mrr.account_id"
        "=cte_mrr_end_of_month_by_account_no_group_by_record_date.mrr_account_id "
        "GROUP BY mrr.account_id "
        "ORDER BY mrr_mrr_end_of_month_by_account_no_group_by DESC;"
    )
    assert query == correct
