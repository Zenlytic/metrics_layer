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


JINJA_BLOCK = "{% if 'USA' in user_attributes['gsam'] %}a{% else %}b{% endif %}"
LOOKER_BLOCK = "{% if _user_attributes['gsam'] contains '8USA' %}a{% else %}b{% endif %}"


def _render(sql, attributes):
    from metrics_layer.core.model.view import View

    return View.jinja_replacements(sql, {"user_attributes": attributes})


@pytest.mark.query
def test_jinja_expression_renders_unchanged():
    assert _render("{{ user_attributes['region'] }}.analytics", {"region": "east"}) == "east.analytics"
    assert _render("{{ user_attributes['region'] }}", {}) == "{{ user_attributes['region'] }}"


@pytest.mark.query
def test_jinja_block_only_template_renders():
    assert "{{" not in JINJA_BLOCK
    assert _render(JINJA_BLOCK, {"gsam": "8USA,2CAN"}) == "a"
    assert _render(JINJA_BLOCK, {"gsam": "2CAN"}) == "b"


@pytest.mark.query
def test_looker_liquid_block_template_renders():
    assert _render(LOOKER_BLOCK, {"gsam": "8USA,2CAN"}) == "a"
    assert _render(LOOKER_BLOCK, {"gsam": "2CAN"}) == "b"


@pytest.mark.query
def test_looker_liquid_sql_quote_filter():
    template = "{{ _user_attributes['region'] | sql_quote }}"
    assert _render(template, {"region": "east"}) == "'east'"
    assert _render(template, {"region": "o'neil"}) == "'o''neil'"


@pytest.mark.query
def test_missing_attribute_returns_raw_template():
    # Fail closed: the raw template is returned, never the else branch
    assert _render(JINJA_BLOCK, {}) == JINJA_BLOCK
    assert _render(LOOKER_BLOCK, {}) == LOOKER_BLOCK
    truthy = "{% if _user_attributes['missing'] %}a{% else %}b{% endif %}"
    assert _render(truthy, {"gsam": "x"}) == truthy
    contains = "{% if _user_attributes['missing'] contains 'x' %}a{% else %}b{% endif %}"
    assert _render(contains, {"gsam": "x"}) == contains


@pytest.mark.query
def test_looker_parameter_template_returns_raw():
    # Validation still flags these fields as Looker parameters: see tests/test_cli.py::test_cli_looker_parameter
    template = "{% if time_._parameter_value == 'seconds' %}\n ${TABLE}.time_second{% endif %}"
    assert _render(template, {"gsam": "x"}) == template


@pytest.mark.query
def test_template_valid_in_both_engines_uses_jinja_result():
    # Valid Jinja and Liquid: Jinja runs first and renders it
    template = "{% if 'USA' in user_attributes['gsam'] %}{{ user_attributes['gsam'] }}{% endif %}"
    assert _render(template, {"gsam": "8USA"}) == "8USA"


@pytest.mark.query
def test_plain_sql_with_trailing_newline_is_unchanged():
    sql = "SELECT * FROM analytics.orders\n"
    assert _render(sql, {"gsam": "x"}) == sql


@pytest.mark.query
def test_jinja_runtime_type_error_returns_raw_not_liquid_branch():
    template = "{% if user_attributes['g'] > 3 %}a{% else %}b{% endif %}"
    assert _render(template, {"g": None}) == template
    assert _render(template, {"g": 5}) == "a"


@pytest.mark.query
def test_looker_style_name_in_valid_jinja_reaches_liquid():
    template = "{% if _user_attributes['gsam'] == '8USA' %}a{% else %}b{% endif %}"
    assert _render(template, {"gsam": "8USA"}) == "a"
    assert _render(template, {"gsam": "2CAN"}) == "b"
    assert _render(template, {}) == template


@pytest.mark.query
def test_looker_template_in_view_sql_table_name(connection, monkeypatch):
    view = connection.project.get_view("other_db_traffic")
    monkeypatch.setitem(
        view._definition,
        "sql_table_name",
        "{% if _user_attributes['gsam'] contains '8USA' %}prd.tbl{% else %}prd.tbl_nogsam_v{% endif %}",
    )
    connection.project.set_user({"gsam": "8USA"})
    try:
        assert view.sql_table_name == "prd.tbl"
        connection.project.set_user({"gsam": "other"})
        assert view.sql_table_name == "prd.tbl_nogsam_v"
    finally:
        connection.project.set_user({})


@pytest.mark.query
def test_looker_template_in_view_derived_table(connection, monkeypatch):
    view = connection.project.get_view("country_detail")
    template = "SELECT * FROM {% if _user_attributes['gsam'] contains '8USA' %}a{% else %}b{% endif %}"
    monkeypatch.setitem(view._definition["derived_table"], "sql", template)
    connection.project.set_user({"gsam": "8USA"})
    try:
        assert view.derived_table_sql == "SELECT * FROM a"
        connection.project.set_user({"gsam": "other"})
        assert view.derived_table_sql == "SELECT * FROM b"
    finally:
        connection.project.set_user({})


@pytest.mark.query
def test_looker_template_in_field_sql_and_filter_value(connection):
    from metrics_layer.core.model.filter import Filter

    field = connection.project.get_field("order_lines.product_name")
    connection.project.set_user({"gsam": "8USA"})
    try:
        assert field.sql_replacement_func(LOOKER_BLOCK) == "a"
        value = field.sql_replacement_func(
            "{% if _user_attributes['gsam'] contains '8USA' %}Italy{% else %}France{% endif %}"
        )
        assert value == "Italy"
        sql = Filter.translate_looker_filters_to_sql(
            "${rain}",
            [{"field": "country", "value": LOOKER_BLOCK}],
            field.view,
            sql_replacement_func=field.sql_replacement_func,
        )
        assert "'b'" not in sql
        assert "{%" not in sql
        assert sql == "case when ${country}='a' then ${rain} end"
    finally:
        connection.project.set_user({})


@pytest.mark.query
def test_query_attributes_alias_in_field_path(connection):
    field = connection.project.get_field("country_detail.rainfall_at_date")
    template = "{% if _query_attributes['dimension_group'] == 'date' %}a{% else %}b{% endif %}"
    assert field.sql_replacement_func(template) == "a"


@pytest.mark.query
def test_derived_table_trailing_newline_hash_inputs(connection, monkeypatch):
    view = connection.project.get_view("country_detail")
    connection.project.set_user({"g": "x"})
    try:
        # No template: untouched, trailing newline kept (sql_hash unchanged)
        monkeypatch.setitem(view._definition["derived_table"], "sql", "SELECT 1\n")
        assert view.derived_table_sql == "SELECT 1\n"
        # Jinja block template: rendered by Jinja, which drops one trailing newline
        monkeypatch.setitem(
            view._definition["derived_table"],
            "sql",
            "SELECT {% if user_attributes['g'] == 'x' %}a{% endif %}\n",
        )
        assert view.derived_table_sql == "SELECT a"
        # Looker Liquid block template: rendered by Liquid, which keeps the trailing newline
        monkeypatch.setitem(
            view._definition["derived_table"],
            "sql",
            "SELECT {% if _user_attributes['g'] contains 'x' %}a{% endif %}\n",
        )
        assert view.derived_table_sql == "SELECT a\n"
    finally:
        connection.project.set_user({})
