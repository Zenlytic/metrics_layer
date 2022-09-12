import pendulum
import pytest

from metrics_layer.core.exceptions import QueryError
from metrics_layer.core import MetricsLayerConnection


def test_dashboard_located(connection):
    dash = connection.get_dashboard("sales_dashboard")

    assert dash is not None
    assert dash.name == "sales_dashboard"
    assert dash.label == "Sales Dashboard (with campaigns)"
    assert dash.layout == "grid"
    assert isinstance(dash.elements(), list)

    first_element = dash.elements()[0]
    assert first_element.title == "First element"
    assert first_element.type == "plot"
    assert first_element.model == "test_model"
    assert first_element.metric == "orders.total_revenue"
    assert first_element.slice_by == ["orders.new_vs_repeat", "order_lines.product_name"]


def test_dashboard_to_dict(connection):
    dash = connection.get_dashboard("sales_dashboard")

    dash_dict = dash.to_dict()
    assert dash_dict["name"] == "sales_dashboard"
    correct = {
        "expression": "equal_to",
        "field": "orders.new_vs_repeat",
        "timezone": None,
        "value": "New",
        "week_start_day": None,
    }
    assert len(dash_dict["filters"]) == 1
    assert dash_dict["filters"][0] == correct
    assert isinstance(dash_dict["elements"], list)

    assert dash_dict["elements"][0]["filters"] == []

    correct = {
        "expression": "not_equal_to",
        "field": "order_lines.product_name",
        "timezone": None,
        "value": "Handbag",
        "week_start_day": None,
    }
    assert len(dash_dict["elements"][-1]["filters"]) == 1
    assert dash_dict["elements"][-1]["filters"][0] == correct

    first_element = dash_dict["elements"][0]
    assert first_element["title"] == "First element"
    assert first_element["type"] == "plot"
    assert first_element["model"] == "test_model"
    assert first_element["metric"] == "orders.total_revenue"
    assert first_element["slice_by"] == ["orders.new_vs_repeat", "order_lines.product_name"]


@pytest.mark.query
def test_dashboard_filter_week_start(fresh_project):
    date_format = "%Y-%m-%dT%H:%M:%S"
    fresh_project._models[0]["week_start_day"] = "sunday"
    connection = MetricsLayerConnection(project=fresh_project, connections=[])
    dash = connection.get_dashboard("sales_dashboard")

    raw_filter_dict = {"field": "orders.order_year", "value": "1 week"}
    dash.filters = [raw_filter_dict]
    dashboard_parsed_filters = dash.parsed_filters()

    last_element = dash.elements()[-1]
    last_element.filters = [raw_filter_dict]
    element_parsed_filters = last_element.parsed_filters()

    start = pendulum.now("UTC").start_of("week").subtract(days=1).strftime(date_format)
    end = pendulum.now("UTC").end_of("week").subtract(days=1).strftime(date_format)
    correct = [
        {"field": "orders.order_year", "value": start, "expression": "greater_or_equal_than"},
        {"field": "orders.order_year", "value": end, "expression": "less_or_equal_than"},
    ]
    for parsed_filters in [dashboard_parsed_filters, element_parsed_filters]:
        assert parsed_filters[0]["field"] == correct[0]["field"]
        assert parsed_filters[0]["expression"].value == correct[0]["expression"]
        assert parsed_filters[0]["value"] == correct[0]["value"]
        assert parsed_filters[1]["field"] == correct[1]["field"]
        assert parsed_filters[1]["expression"].value == correct[1]["expression"]
        assert parsed_filters[1]["value"] == correct[1]["value"]


@pytest.mark.query
def test_dashboard_filter_timezone(fresh_project):
    date_format = "%Y-%m-%dT%H:%M:%S"
    fresh_project.set_timezone("Pacific/Apia")
    connection = MetricsLayerConnection(project=fresh_project, connections=[])
    dash = connection.get_dashboard("sales_dashboard")

    raw_filter_dict = {"field": "orders.order_date", "value": "week to date"}
    dash.filters = [raw_filter_dict]
    dashboard_parsed_filters = dash.parsed_filters()

    last_element = dash.elements()[-1]
    last_element.filters = [raw_filter_dict]
    element_parsed_filters = last_element.parsed_filters()

    # These are 24 hours apart so this test should always fail if we get the wrong timezone
    start = pendulum.now("Pacific/Apia").start_of("week").strftime(date_format)
    end = pendulum.now("Pacific/Apia").subtract(days=1).end_of("day").strftime(date_format)
    wrong_end = pendulum.now("Pacific/Niue").subtract(days=1).end_of("day").strftime(date_format)
    correct = [
        {"field": "orders.order_date", "value": start, "expression": "greater_or_equal_than"},
        {"field": "orders.order_date", "value": end, "expression": "less_or_equal_than"},
    ]
    for parsed_filters in [dashboard_parsed_filters, element_parsed_filters]:
        assert parsed_filters[0]["field"] == correct[0]["field"]
        assert parsed_filters[0]["expression"].value == correct[0]["expression"]
        assert parsed_filters[0]["value"] == correct[0]["value"]
        assert parsed_filters[1]["field"] == correct[1]["field"]
        assert parsed_filters[1]["expression"].value == correct[1]["expression"]
        assert parsed_filters[1]["value"] == correct[1]["value"]
        assert parsed_filters[1]["value"] != wrong_end


@pytest.mark.query
@pytest.mark.parametrize(
    "raw_filter_dict",
    [
        {"field": "customers.gender", "value": "Male"},
        {"field": "customers.gender", "value": "-Male"},
        {"field": "customers.gender", "value": "-Ma%"},
        {"field": "customers.gender", "value": "-%Ma"},
        {"field": "customers.gender", "value": "-%ale%"},
        {"field": "customers.gender", "value": "Fe%"},
        {"field": "customers.gender", "value": "%Fe"},
        {"field": "customers.gender", "value": "%male%"},
        {"field": "orders.revenue_dimension", "value": "=100"},
        {"field": "orders.revenue_dimension", "value": ">100"},
        {"field": "orders.revenue_dimension", "value": "<100"},
        {"field": "orders.revenue_dimension", "value": "<=120"},
        {"field": "orders.revenue_dimension", "value": ">=120"},
        {"field": "orders.revenue_dimension", "value": "!=120"},
        {"field": "orders.revenue_dimension", "value": "<>120"},
        {"field": "orders.order_month", "value": "after 2021-02-03"},
        {"field": "orders.order_month", "value": "before 2021-02-03"},
        {"field": "orders.order_date", "value": "today"},
        {"field": "orders.order_date", "value": "yesterday"},
        {"field": "orders.order_week", "value": "this week"},
        {"field": "orders.order_month", "value": "this month"},
        {"field": "orders.order_month", "value": "this quarter"},
        {"field": "orders.order_month", "value": "this year"},
        {"field": "orders.order_week", "value": "last week"},
        {"field": "orders.order_month", "value": "last month"},
        {"field": "orders.order_month", "value": "last quarter"},
        {"field": "orders.order_month", "value": "last year"},
        {"field": "orders.order_week", "value": "week to date"},
        {"field": "orders.order_month", "value": "month to date"},
        {"field": "orders.order_quarter", "value": "quarter to date"},
        {"field": "orders.order_year", "value": "year to date"},
        {"field": "orders.order_week", "value": "last week to date"},
        {"field": "orders.order_week", "value": "52 weeks ago to date"},
        {"field": "orders.order_month", "value": "12 months ago to date"},
        {"field": "orders.order_year", "value": "1 year ago to date"},
        {"field": "orders.order_year", "value": "1 year ago for 3 months"},
        {"field": "orders.order_year", "value": "1 year ago for 30 days"},
        {"field": "orders.order_year", "value": "2 years ago"},
        {"field": "orders.order_year", "value": "3 months"},
        {"field": "orders.order_year", "value": "1 week"},
        {"field": "orders.order_year", "value": "2 days"},
        {"field": "customers.gender", "value": "Male, Female"},
        {"field": "customers.gender", "value": "-Male, -Female"},
        {"field": "customers.gender", "value": "-NULL"},
        {"field": "customers.gender", "value": "NULL"},
        {"field": "customers.is_churned", "value": "TRUE"},
        {"field": "customers.is_churned", "value": True},
        {"field": "customers.is_churned", "value": False},
        {"field": "customers.gender", "value": "-Male, Female"},
    ],
)
def test_dashboard_filter_processing(connection, raw_filter_dict):
    dash = connection.get_dashboard("sales_dashboard")
    dash.filters = [raw_filter_dict]

    expression_lookup = {
        "Male": "equal_to",
        "-Male": "not_equal_to",
        "-Ma%": "does_not_start_with_case_insensitive",
        "-%Ma": "does_not_end_with_case_insensitive",
        "-%ale%": "does_not_contain_case_insensitive",
        "Fe%": "starts_with_case_insensitive",
        "%Fe": "ends_with_case_insensitive",
        "%male%": "contains_case_insensitive",
        "=100": "equal_to",
        ">100": "greater_than",
        "<100": "less_than",
        "<=120": "less_or_equal_than",
        ">=120": "greater_or_equal_than",
        "!=120": "not_equal_to",
        "<>120": "not_equal_to",
        "Male, Female": "isin",
        "-Male, -Female": "isnotin",
        "-NULL": "is_not_null",
        "NULL": "is_null",
        "TRUE": "equal_to",
        True: "equal_to",
        False: "equal_to",
        "after 2021-02-03": "greater_or_equal_than",
        "before 2021-02-03": "less_or_equal_than",
        "today": "greater_or_equal_than",
        "yesterday": "greater_or_equal_than",
        "this week": "greater_or_equal_than",
        "this month": "greater_or_equal_than",
        "this quarter": "greater_or_equal_than",
        "this year": "greater_or_equal_than",
        "last week": "greater_or_equal_than",
        "last month": "greater_or_equal_than",
        "last quarter": "greater_or_equal_than",
        "last year": "greater_or_equal_than",
        "week to date": "greater_or_equal_than",
        "month to date": "greater_or_equal_than",
        "quarter to date": "greater_or_equal_than",
        "year to date": "greater_or_equal_than",
        "last week to date": "greater_or_equal_than",
        "52 weeks ago to date": "greater_or_equal_than",
        "12 months ago to date": "greater_or_equal_than",
        "1 year ago to date": "greater_or_equal_than",
        "1 year ago for 3 months": "greater_or_equal_than",
        "1 year ago for 30 days": "greater_or_equal_than",
        "2 years ago": "greater_or_equal_than",
        "3 months": "greater_or_equal_than",
        "1 week": "greater_or_equal_than",
        "2 days": "greater_or_equal_than",
        "1 quarter": "greater_or_equal_than",
    }
    date_format = "%Y-%m-%dT%H:%M:%S"
    value_lookup = {
        "Male": "Male",
        "-Male": "Male",
        "-Ma%": "Ma",
        "-%Ma": "Ma",
        "-%ale%": "ale",
        "Fe%": "Fe",
        "%Fe": "Fe",
        "%male%": "male",
        "=100": 100,
        ">100": 100,
        "<100": 100,
        "<=120": 120,
        ">=120": 120,
        "!=120": 120,
        "<>120": 120,
        "Male, Female": ["Male", "Female"],
        "-Male, -Female": ["Male", "Female"],
        "-NULL": None,
        "NULL": None,
        "TRUE": True,
        True: True,
        False: False,
        "after 2021-02-03": "2021-02-03T00:00:00",
        "before 2021-02-03": "2021-02-03T00:00:00",
        "today": pendulum.now("UTC").start_of("day").strftime(date_format),
        "yesterday": pendulum.now("UTC").subtract(days=1).start_of("day").strftime(date_format),
        "this week": pendulum.now("UTC").subtract(weeks=0).start_of("week").strftime(date_format),
        "this month": pendulum.now("UTC").subtract(months=0).start_of("month").strftime(date_format),
        "this quarter": pendulum.now("UTC").subtract(months=0).first_of("quarter").strftime(date_format),
        "this year": pendulum.now("UTC").subtract(years=0).start_of("year").strftime(date_format),
        "last week": pendulum.now("UTC").subtract(weeks=1).start_of("week").strftime(date_format),
        "last month": pendulum.now("UTC").subtract(months=1).start_of("month").strftime(date_format),
        "last quarter": pendulum.now("UTC").subtract(months=3).first_of("quarter").strftime(date_format),
        "last year": pendulum.now("UTC").subtract(years=1).start_of("year").strftime(date_format),
        "week to date": pendulum.now("UTC").subtract(weeks=0).start_of("week").strftime(date_format),
        "month to date": pendulum.now("UTC").subtract(months=0).start_of("month").strftime(date_format),
        "quarter to date": pendulum.now("UTC").subtract(months=0).first_of("quarter").strftime(date_format),
        "year to date": pendulum.now("UTC").subtract(years=0).start_of("year").strftime(date_format),
        "last week to date": pendulum.now("UTC").subtract(weeks=1).start_of("week").strftime(date_format),
        "52 weeks ago to date": pendulum.now("UTC").subtract(weeks=52).start_of("week").strftime(date_format),
        "12 months ago to date": pendulum.now("UTC")
        .subtract(months=12)
        .start_of("month")
        .strftime(date_format),
        "1 year ago to date": pendulum.now("UTC").subtract(years=1).start_of("year").strftime(date_format),
        "1 year ago for 3 months": pendulum.now("UTC")
        .subtract(years=1)
        .start_of("year")
        .strftime(date_format),
        "1 year ago for 30 days": pendulum.now("UTC")
        .subtract(years=1)
        .start_of("year")
        .strftime(date_format),
        "2 years ago": pendulum.now("UTC").subtract(years=2).start_of("year").strftime(date_format),
        "3 months": pendulum.now("UTC").subtract(months=2).start_of("month").strftime(date_format),
        "1 week": pendulum.now("UTC").start_of("week").strftime(date_format),
        "2 days": pendulum.now("UTC").subtract(days=1).start_of("day").strftime(date_format),
        "1 quarter": pendulum.now("UTC").first_of("quarter").strftime(date_format),
    }

    second_value_lookup = {
        "today": pendulum.now("UTC").end_of("day").strftime(date_format),
        "yesterday": pendulum.now("UTC").subtract(days=1).end_of("day").strftime(date_format),
        "this week": pendulum.now("UTC").end_of("week").strftime(date_format),
        "this month": pendulum.now("UTC").end_of("month").strftime(date_format),
        "this quarter": pendulum.now("UTC").last_of("quarter").strftime(date_format),
        "this year": pendulum.now("UTC").end_of("year").strftime(date_format),
        "last week": pendulum.now("UTC").subtract(weeks=1).end_of("week").strftime(date_format),
        "last month": pendulum.now("UTC").subtract(months=1).end_of("month").strftime(date_format),
        "last quarter": pendulum.now("UTC").subtract(months=3).last_of("quarter").strftime(date_format),
        "last year": pendulum.now("UTC").subtract(years=1).end_of("year").strftime(date_format),
        "week to date": pendulum.now("UTC")
        .subtract(days=1 if pendulum.now("UTC").day_of_week != 1 else 0)
        .end_of("day")
        .strftime(date_format),
        "month to date": pendulum.now("UTC")
        .subtract(days=1 if pendulum.now("UTC").day != 1 else 0)
        .end_of("day")
        .strftime(date_format),
        "quarter to date": pendulum.now("UTC").subtract(days=1).end_of("day").strftime(date_format),
        "year to date": pendulum.now("UTC").subtract(days=1).end_of("day").strftime(date_format),
        "last week to date": pendulum.now("UTC")
        .subtract(weeks=1)
        .start_of("week")
        .add(
            days=(pendulum.now("UTC") - pendulum.now("UTC").start_of("week")).days - 1
            if pendulum.now("UTC").day_of_week != 1
            else 0
        )
        .end_of("day")
        .strftime(date_format),
        "52 weeks ago to date": pendulum.now("UTC")
        .subtract(weeks=52)
        .start_of("week")
        .add(
            days=(pendulum.now("UTC") - pendulum.now("UTC").start_of("week")).days - 1
            if pendulum.now("UTC").day_of_week != 1
            else 0
        )
        .end_of("day")
        .strftime(date_format),
        "12 months ago to date": pendulum.now("UTC")
        .subtract(months=12)
        .start_of("month")
        .add(
            days=(pendulum.now("UTC") - pendulum.now("UTC").start_of("month")).days - 1
            if pendulum.now("UTC").day != 1
            else 0
        )
        .end_of("day")
        .strftime(date_format),
        "1 year ago to date": pendulum.now("UTC")
        .subtract(years=1)
        .start_of("year")
        .add(days=(pendulum.now("UTC") - pendulum.now("UTC").start_of("year")).days - 1)
        .end_of("day")
        .strftime(date_format),
        "1 year ago for 3 months": pendulum.now("UTC")
        .subtract(years=1)
        .start_of("year")
        .add(months=2)
        .end_of("month")
        .strftime(date_format),
        "1 year ago for 30 days": pendulum.now("UTC")
        .subtract(years=1)
        .start_of("year")
        .add(days=29)
        .end_of("day")
        .strftime(date_format),
        "2 years ago": pendulum.now("UTC").subtract(years=2).end_of("year").strftime(date_format),
        "3 months": pendulum.now("UTC").end_of("month").strftime(date_format),
        "1 week": pendulum.now("UTC").end_of("week").strftime(date_format),
        "2 days": pendulum.now("UTC").end_of("day").strftime(date_format),
        "1 quarter": pendulum.now("UTC").last_of("quarter").strftime(date_format),
    }
    if raw_filter_dict["value"] == "-Male, Female":
        with pytest.raises(QueryError) as exc_info:
            dash.parsed_filters()
        assert exc_info.value
    else:
        parsed_filters = dash.parsed_filters()
        assert len(parsed_filters) in {1, 2}
        assert parsed_filters[0]["field"] == raw_filter_dict["field"]
        assert parsed_filters[0]["expression"].value == expression_lookup[raw_filter_dict["value"]]
        assert parsed_filters[0]["value"] == value_lookup[raw_filter_dict["value"]]
        if raw_filter_dict["value"] in second_value_lookup or len(parsed_filters) == 2:
            assert parsed_filters[1]["field"] == raw_filter_dict["field"]
            assert parsed_filters[1]["expression"].value == "less_or_equal_than"
            assert parsed_filters[1]["value"] == second_value_lookup[raw_filter_dict["value"]]


@pytest.mark.parametrize(
    "raw_filter_dict",
    [
        {"field": "customers.gender", "value": "Male"},
        {"field": "customers.gender", "value": "-Male"},
        {"field": "orders.revenue_dimension", "value": "=100"},
        {"field": "orders.revenue_dimension", "value": ">100"},
        {"field": "orders.revenue_dimension", "value": "<100"},
        {"field": "orders.revenue_dimension", "value": "<=120"},
        {"field": "orders.revenue_dimension", "value": ">=120"},
        {"field": "orders.revenue_dimension", "value": "!=120"},
        {"field": "orders.revenue_dimension", "value": "<>120"},
        {"field": "customers.gender", "value": "Male, Female"},
        {"field": "customers.gender", "value": "-Male, -Female"},
        {"field": "customers.gender", "value": "-NULL"},
        {"field": "customers.gender", "value": "NULL"},
        {"field": "customers.gender", "value": "-Male, Female"},
    ],
)
def test_dashboard_element_filter_processing(connection, raw_filter_dict):
    dash = connection.get_dashboard("sales_dashboard")
    element = dash.elements()[0]
    element.filters = [raw_filter_dict]

    expression_lookup = {
        "Male": "equal_to",
        "-Male": "not_equal_to",
        "=100": "equal_to",
        ">100": "greater_than",
        "<100": "less_than",
        "<=120": "less_or_equal_than",
        ">=120": "greater_or_equal_than",
        "!=120": "not_equal_to",
        "<>120": "not_equal_to",
        "Male, Female": "isin",
        "-Male, -Female": "isnotin",
        "-NULL": "is_not_null",
        "NULL": "is_null",
    }
    value_lookup = {
        "Male": "Male",
        "-Male": "Male",
        "=100": 100,
        ">100": 100,
        "<100": 100,
        "<=120": 120,
        ">=120": 120,
        "!=120": 120,
        "<>120": 120,
        "Male, Female": ["Male", "Female"],
        "-Male, -Female": ["Male", "Female"],
        "-NULL": None,
        "NULL": None,
    }

    if raw_filter_dict["value"] == "-Male, Female":
        with pytest.raises(QueryError) as exc_info:
            element.parsed_filters()
        assert exc_info.value

    else:
        parsed_filters = element.parsed_filters()
        assert len(parsed_filters) == 1
        assert parsed_filters[0]["field"] == raw_filter_dict["field"]
        assert parsed_filters[0]["expression"].value == expression_lookup[raw_filter_dict["value"]]
        assert parsed_filters[0]["value"] == value_lookup[raw_filter_dict["value"]]
