import pytest


@pytest.mark.query
def test_query_user_attribute_in_sql(connection):
    connection.project.set_user({"user_lang": "us-en"})

    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["product_name_lang", "product_name"],
    )

    correct = (
        "SELECT LOOKUP(order_lines.product_name, 'us-en' ) as"
        " order_lines_product_name_lang,order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY LOOKUP(order_lines.product_name, 'us-en'"
        " ),order_lines.product_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_user_attribute_in_derived_table_sql(connection):
    connection.project.set_user({"owned_region": "Europe"})
    query = connection.get_sql_query(metrics=["avg_rainfall"], dimensions=[])

    correct = (
        "SELECT AVG(country_detail.rain) as country_detail_avg_rainfall FROM (SELECT * FROM"
        " ANALYTICS.COUNTRY_DETAIL WHERE 'Europe' = COUNTRY_DETAIL.REGION) as country_detail ORDER BY"
        " country_detail_avg_rainfall DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_user_attribute_in_sql_table_name(connection):
    connection.project.set_user({"db_name": "q3lm13dfa"})
    query = connection.get_sql_query(metrics=[], dimensions=["other_traffic_id"])

    correct = (
        "SELECT other_db_traffic.id as other_db_traffic_other_traffic_id FROM q3lm13dfa.analytics.traffic"
        " other_db_traffic GROUP BY other_db_traffic.id ORDER BY other_db_traffic_other_traffic_id ASC NULLS"
        " LAST;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_user_attribute_in_filter_sql(connection):
    connection.project.set_user({"owned_region": "Europe", "country_options": "Italy, FRANCE"})
    query = connection.get_sql_query(metrics=["avg_rainfall_adj"], dimensions=[])

    correct = (
        "SELECT AVG(case when country_detail.country IN ('Italy','FRANCE') then country_detail.rain end) as"
        " country_detail_avg_rainfall_adj FROM (SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE 'Europe' ="
        " COUNTRY_DETAIL.REGION) as country_detail ORDER BY country_detail_avg_rainfall_adj DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_user_attribute_in_filter_sql_error_state(connection):
    # When the user attribute is not set, the query will not fill in the user attribute
    query = connection.get_sql_query(metrics=["avg_rainfall_adj"], dimensions=[])

    correct = (
        "SELECT AVG(case when country_detail.country='{{ user_attributes[''country_options''] }}' then"
        " country_detail.rain end) as country_detail_avg_rainfall_adj FROM (SELECT * FROM"
        " ANALYTICS.COUNTRY_DETAIL WHERE '{{ user_attributes['owned_region'] }}' = COUNTRY_DETAIL.REGION) as"
        " country_detail ORDER BY country_detail_avg_rainfall_adj DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_query_attribute_dimension_group(connection):
    connection.project.set_user({"owned_region": "Europe"})
    query = connection.get_sql_query(metrics=["avg_rainfall"], dimensions=["rainfall_at_date"])

    correct = (
        "SELECT DATE_TRUNC('DAY', case when 'date' = 'raw' then country_detail.rain_date when 'date' = 'date'"
        " then time_bucket('1 day', country_detail.rain_date) when 'date' = 'week' then time_bucket('1 week',"
        " country_detail.rain_date) when 'date' = 'month' then time_bucket('1 month',"
        " country_detail.rain_date) end) as country_detail_rainfall_at_date,AVG(country_detail.rain) as"
        " country_detail_avg_rainfall FROM (SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE 'Europe' ="
        " COUNTRY_DETAIL.REGION) as country_detail GROUP BY DATE_TRUNC('DAY', case when 'date' = 'raw' then"
        " country_detail.rain_date when 'date' = 'date' then time_bucket('1 day', country_detail.rain_date)"
        " when 'date' = 'week' then time_bucket('1 week', country_detail.rain_date) when 'date' = 'month'"
        " then time_bucket('1 month', country_detail.rain_date) end) ORDER BY country_detail_avg_rainfall"
        " DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert query == correct
