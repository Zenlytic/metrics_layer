import pytest


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
    assert first_element.explore == "order_lines_all"
    assert first_element.metric == "orders.total_revenue"
    assert first_element.slice_by == ["orders.new_vs_repeat", "order_lines.product_name"]


def test_dashboard_to_dict(connection):
    dash = connection.get_dashboard("sales_dashboard")

    dash_dict = dash.to_dict()
    assert dash_dict["name"] == "sales_dashboard"
    correct = {
        "explore": "order_lines_all",
        "expression": "equal_to",
        "field": "orders.new_vs_repeat",
        "value": "New",
    }
    assert len(dash_dict["filters"]) == 1
    assert dash_dict["filters"][0] == correct
    assert isinstance(dash_dict["elements"], list)

    assert dash_dict["elements"][0]["filters"] == []

    correct = {"expression": "not_equal_to", "field": "order_lines.product_name", "value": "Handbag"}
    assert len(dash_dict["elements"][-1]["filters"]) == 1
    assert dash_dict["elements"][-1]["filters"][0] == correct

    first_element = dash_dict["elements"][0]
    assert first_element["title"] == "First element"
    assert first_element["type"] == "plot"
    assert first_element["model"] == "test_model"
    assert first_element["explore"] == "order_lines_all"
    assert first_element["metric"] == "orders.total_revenue"
    assert first_element["slice_by"] == ["orders.new_vs_repeat", "order_lines.product_name"]


@pytest.mark.mm
@pytest.mark.parametrize(
    "raw_filter_dict",
    [
        {"explore": "orders", "field": "customers.gender", "value": "Male"},
        {"explore": "orders", "field": "customers.gender", "value": "-Male"},
        {"explore": "orders", "field": "customers.gender", "value": "-Ma%"},
        {"explore": "orders", "field": "customers.gender", "value": "-%Ma"},
        {"explore": "orders", "field": "customers.gender", "value": "-%ale%"},
        {"explore": "orders", "field": "customers.gender", "value": "Fe%"},
        {"explore": "orders", "field": "customers.gender", "value": "%Fe"},
        {"explore": "orders", "field": "customers.gender", "value": "%male%"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": "=100"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": ">100"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": "<100"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": "<=120"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": ">=120"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": "!=120"},
        {"explore": "orders", "field": "orders.revenue_dimension", "value": "<>120"},
        {"explore": "orders", "field": "orders.order_month", "value": "after 2021-02-03"},
        {"explore": "orders", "field": "orders.order_month", "value": "before 2021-02-03"},
        # {"explore": "orders", "field": "orders.order_date", "value": "yesterday"},
        # {"explore": "orders", "field": "orders.order_week", "value": "last week"},
        # {"explore": "orders", "field": "orders.order_month", "value": "last month"},
        # {"explore": "orders", "field": "orders.order_month", "value": "last quarter"},
        # {"explore": "orders", "field": "orders.order_month", "value": "last year"},
        # {"explore": "orders", "field": "orders.order_week", "value": "week to date"},
        # {"explore": "orders", "field": "orders.order_month", "value": "month to date"},
        # {"explore": "orders", "field": "orders.order_quarter", "value": "quarter to date"},
        # {"explore": "orders", "field": "orders.order_year", "value": "year to date"},
        # {"explore": "orders", "field": "orders.order_week", "value": "last week to date"},
        # {"explore": "orders", "field": "orders.order_month", "value": "last month to date"},
        # {"explore": "orders", "field": "orders.order_quarter", "value": "last quarter to date"},
        # {"explore": "orders", "field": "orders.order_year", "value": "last year to date"},
        # {"explore": "orders", "field": "orders.order_week", "value": "52 weeks ago to date"},
        # {"explore": "orders", "field": "orders.order_month", "value": "12 months ago to date"},
        # {"explore": "orders", "field": "orders.order_quarter", "value": "4 quarters ago to date"},
        # {"explore": "orders", "field": "orders.order_year", "value": "1 year ago to date"},
        # {"explore": "orders", "field": "orders.order_year", "value": "1 year ago for 3 months"},
        # {"explore": "orders", "field": "orders.order_year", "value": "1 year ago for 30 days"},
        # {"explore": "orders", "field": "orders.order_year", "value": "2 years ago"},
        # {"explore": "orders", "field": "orders.order_year", "value": "2 years"},
        {"explore": "orders", "field": "customers.gender", "value": "Male, Female"},
        {"explore": "orders", "field": "customers.gender", "value": "-Male, -Female"},
        {"explore": "orders", "field": "customers.gender", "value": "-NULL"},
        {"explore": "orders", "field": "customers.gender", "value": "NULL"},
        {"explore": "orders", "field": "customers.is_churned", "value": True},
        {"explore": "orders", "field": "customers.is_churned", "value": False},
        {"explore": "orders", "field": "customers.gender", "value": "-Male, Female"},
        {"field": "customers.gender", "value": "BREAK_ON_EXPLORE"},
    ],
)
def test_dashboard_filter_processing(connection, raw_filter_dict):
    dash = connection.get_dashboard("sales_dashboard")
    dash.filters = [raw_filter_dict, raw_filter_dict]

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
        True: "equal_to",
        False: "equal_to",
        "after 2021-02-03": "greater_or_equal_than",
        "before 2021-02-03": "less_or_equal_than",
    }
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
        True: True,
        False: False,
        "after 2021-02-03": "2021-02-03T00:00:00",
        "before 2021-02-03": "2021-02-03T00:00:00",
    }

    if raw_filter_dict["value"] in {"-Male, Female", "BREAK_ON_EXPLORE"}:
        with pytest.raises(ValueError) as exc_info:
            dash.parsed_filters()
        assert exc_info.value

    else:
        parsed_filters = dash.parsed_filters()
        assert len(parsed_filters) == 2
        assert parsed_filters[0]["explore"] == "orders"
        assert parsed_filters[0]["field"] == raw_filter_dict["field"]
        assert parsed_filters[0]["expression"].value == expression_lookup[raw_filter_dict["value"]]
        assert parsed_filters[0]["value"] == value_lookup[raw_filter_dict["value"]]


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
        with pytest.raises(ValueError) as exc_info:
            element.parsed_filters()
        assert exc_info.value

    else:
        parsed_filters = element.parsed_filters()
        assert len(parsed_filters) == 1
        assert parsed_filters[0]["field"] == raw_filter_dict["field"]
        assert parsed_filters[0]["expression"].value == expression_lookup[raw_filter_dict["value"]]
        assert parsed_filters[0]["value"] == value_lookup[raw_filter_dict["value"]]
