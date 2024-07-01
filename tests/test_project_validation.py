import json
from copy import deepcopy

import pytest
import ruamel.yaml

from metrics_layer.core.parse.project_reader_base import Str


def _get_view_by_name(project, view_name):
    for view in project._views:
        if view["name"] == view_name:
            return json.loads(json.dumps(view))
    raise ValueError(f"View {view_name} not found in project views")


def _get_field_by_name(view, field_name):
    for field in view["fields"]:
        if field["name"] == field_name:
            return field
    raise ValueError(f"Field {field_name} not found in view {view['name']}")


@pytest.mark.validation
def test_validation_with_no_replaced_objects(connection):
    project = connection.project
    response = project.validate_with_replaced_objects(replaced_objects=[])
    assert response == []


# Note: line / column validation only works if the property is
# read from the YAML file, not injected like this
@pytest.mark.validation
@pytest.mark.parametrize(
    "name,value,errors",
    [
        (
            "name",
            "@mytest",
            [
                {
                    "message": (
                        "Model name: @mytest is invalid. Please reference the naming conventions (only"
                        " letters, numbers, or underscores)"
                    ),
                    "model_name": "@mytest",
                    "line": 2,
                    "column": 6,
                }
            ],
        ),
        (
            "name",
            "@mytest_plain_str_passed",
            [
                {
                    "message": (
                        "Model name: @mytest_plain_str_passed is invalid. Please reference the naming"
                        " conventions (only letters, numbers, or underscores)"
                    ),
                    "model_name": "@mytest_plain_str_passed",
                    "line": None,
                    "column": None,
                }
            ],
        ),
        (
            "mappings",
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_fake"]}},
            [
                {
                    "column": None,
                    "field_name": "sessions.utm_fake",
                    "line": None,
                    "message": (
                        "In the mapping myfield in the model test_model, the Field "
                        "utm_fake not found in view sessions, please check that this "
                        "field exists AND that you have access to it. \n"
                        "\n"
                        "If this is a dimension group specify the group parameter, if not "
                        "already specified, for example, with a dimension group named "
                        "'order' with timeframes: [raw, date, month] specify 'order_raw' "
                        "or 'order_date' or 'order_month'"
                    ),
                    "model_name": "test_model",
                }
            ],
        ),
    ],
)
def test_validation_model_with_fully_qualified_results(connection, name, value, errors):
    project = connection.project
    model = deepcopy(project._models[0])
    if value == "@mytest":
        value = Str(value)
        value.lc = ruamel.yaml.comments.LineCol()
        value.lc.line = 2
        value.lc.col = 6

    model[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[model])

    assert response == errors


# Note: line / column validation only works if the property is
# read from the YAML file, not injected like this
@pytest.mark.validation
@pytest.mark.parametrize(
    "name,value,errors",
    [
        (
            "label",
            None,
            [
                {
                    "message": "The label property, None must be a string in the view order_lines",
                    "view_name": "order_lines",
                    "line": None,
                    "column": None,
                }
            ],
        )
    ],
)
def test_validation_view_with_fully_qualified_results(connection, name, value, errors):
    project = connection.project
    view = _get_view_by_name(project, "order_lines")
    view[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[view])

    print(response)
    assert response == errors


# Note: line / column validation only works if the property is
# read from the YAML file, not injected like this
@pytest.mark.validation
@pytest.mark.parametrize(
    "field_name,property_name,value,errors",
    [
        (
            "total_item_revenue",
            "canon_date",
            "fake",
            [
                {
                    "message": "Canon date order_lines.fake is unreachable in field total_item_revenue.",
                    "view_name": "order_lines",
                    "field_name": "total_item_revenue",
                    "line": 346,
                    "column": 8,
                }
            ],
        )
    ],
)
def test_validation_field_with_fully_qualified_results(connection, field_name, property_name, value, errors):
    project = connection.project
    view = _get_view_by_name(project, "order_lines")

    if property_name == "__ADD__":
        field = value
    else:
        field = _get_field_by_name(view, field_name)

    if value == "__POP__":
        field.pop(property_name)
    elif property_name == "__ADD__":
        view["fields"].append(value)
    elif isinstance(property_name, tuple):
        for p, v in zip(property_name, value):
            field[p] = v
    elif value == "fake":
        value = Str(value)
        value.lc = ruamel.yaml.comments.LineCol()
        value.lc.line = 346
        value.lc.col = 8
        field[property_name] = value
    else:
        field[property_name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[view])

    print(response)
    assert response == errors


@pytest.mark.validation
@pytest.mark.parametrize(
    "name,value,errors",
    [
        (
            "name",
            "@mytest",
            [
                "Model name: @mytest is invalid. Please reference the naming conventions (only letters,"
                " numbers, or underscores)"
            ],
        ),
        ("connection", None, ["The connection property, None must be a string in the model test_model"]),
        ("connection", 1, ["The connection property, 1 must be a string in the model test_model"]),
        ("connection", "test", []),
        ("label", None, ["The label property, None must be a string in the model test_model"]),
        ("label", "My Model!", []),
        (
            "fiscal_month_offset",
            "3 months",
            ["The fiscal_month_offset property, 3 months must be an integer in the model test_model"],
        ),
        (
            "fiscal_month_offset",
            2,
            [],
        ),
        (
            "week_start_day",
            "sundae",
            [
                "The week_start_day property, sundae must be one of ['monday', 'tuesday', 'wednesday',"
                " 'thursday', 'friday', 'saturday', 'sunday'] in the model test_model"
            ],
        ),
        (
            "week_start_day",
            None,
            [
                "The week_start_day property, None must be one of ['monday', 'tuesday', 'wednesday',"
                " 'thursday', 'friday', 'saturday', 'sunday'] in the model test_model"
            ],
        ),
        ("week_start_day", "sunday", []),
        (
            "timezone",
            None,
            [
                "The timezone property, None must be a valid timezone in the model test_model. Valid"
                " timezones can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
            ],
        ),
        (
            "timezone",
            "ET",
            [
                "The timezone property, ET must be a valid timezone in the model test_model. Valid timezones"
                " can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
            ],
        ),
        ("timezone", "America/New_York", []),
        (
            "default_convert_tz",
            "yes",
            ["The default_convert_tz property, yes must be a boolean in the model test_model"],
        ),
        (
            "default_convert_tz",
            None,
            ["The default_convert_tz property, None must be a boolean in the model test_model"],
        ),
        ("default_convert_tz", True, []),
        (
            "default_tz",
            True,
            [
                "Property default_tz is present on Model test_model, but it is not a valid property. Did you"
                " mean default_convert_tz?"
            ],
        ),
        (
            "dcqwmwfldqw",
            True,
            ["Property dcqwmwfldqw is present on Model test_model, but it is not a valid property."],
        ),
        (
            "access_grants",
            None,
            [
                "The access_grants property, None must be a list in the model test_model",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [1],
            [
                (
                    "All access_grants in the access_grants property must be dictionaries in the "
                    "model test_model"
                ),
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test"}],
            [
                "Access Grant test missing required key user_attribute in the model test_model",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"model": "test"}],
            [
                "Access Grant missing required key name in the model test_model",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "test", "allowed_values": "test"}],
            [
                "The allowed_values property, test must be a list in the Access Grant test",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "test", "allowed_values": [1, 2]}],
            [
                "All values in the allowed_values property must be strings in the Access Grant test",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": None, "allowed_values": ["1", "2"]}],
            [
                "The user_attribute property, None must be a string in the Access Grant test",
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [
                {
                    "name": "test",
                    "user_attribute": "products",
                    "allowed_values": ["blush", "eyeliner"],
                    "allowed_val": "test",
                }
            ],
            [
                (
                    "Property allowed_val is present on Access Grant test, but it is not a valid property."
                    " Did you mean allowed_values?"
                ),
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "products", "allowed_values": ["blush", "eyeliner"]}],
            [
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_field in the "
                    "required_access_grants property does not exist in model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in model test_model"
                ),
            ],
        ),
        (
            "mappings",
            [],
            ["The mappings property, [] must be a dictionary in the model test_model"],
        ),
        (
            "mappings",
            {"date": {"fields": []}},
            [
                "The mapping name date is a reserved name and cannot be used as a mapping name in the model"
                " test_model"
            ],
        ),
        (
            "mappings",
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_fake"]}},
            [
                "In the mapping myfield in the model test_model, the Field utm_fake not found in view"
                " sessions, please check that this field exists AND that you have access to it. \n\nIf this"
                " is a dimension group specify the group parameter, if not already specified, for example,"
                " with a dimension group named 'order' with timeframes: [raw, date, month] specify"
                " 'order_raw' or 'order_date' or 'order_month'"
            ],
        ),
        (
            "mappings",
            {"myfield": {"fields": "orders.sub_channel"}},
            [
                "The fields property, orders.sub_channel must be a list in the mapping myfield in the model"
                " test_model"
            ],
        ),
        (
            "mappings",
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_campaign"], "description": 1}},
            ["The description property, 1 must be a string in the mapping myfield in the model test_model"],
        ),
        (
            "mappings",
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_campaign"], "group_label": 1}},
            ["The group_label property, 1 must be a string in the mapping myfield in the model test_model"],
        ),
        (
            "mappings",
            {
                "myfield": {
                    "fields": ["orders.sub_channel", "sessions.utm_campaign"],
                    "desc": "This is a test",
                }
            },
            ["Property desc is present on Mapping myfield, but it is not a valid property."],
        ),
        (
            "mappings",
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_campaign"], "label": "My label"}},
            [],
        ),
        (
            "mappings",
            {
                "myfield": {
                    "fields": ["orders.sub_channel", "sessions.utm_campaign"],
                    "description": "This is a test",
                    "group_label": "Test",
                    "link": "https://google.com",
                },
            },
            [],
        ),
    ],
)
def test_validation_with_replaced_model_properties(connection, name, value, errors):
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))
    model[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[model])

    assert [e["message"] for e in response] == errors


@pytest.mark.validation
@pytest.mark.parametrize(
    "name,value,errors",
    [
        (
            "model_name",
            None,
            [
                "Could not find a model in the view order_lines. Use the model_name property to specify the"
                " model."
            ],
        ),
        (
            "model_name",
            "missing_model",
            [
                "Could not find a model in the view order_lines. Use the model_name property to specify the"
                " model."
            ],
        ),
        (
            "name",
            "@mytest",
            [
                "View name: @mytest is invalid. Please reference the naming conventions (only letters,"
                " numbers, or underscores)"
            ],
        ),
        ("label", None, ["The label property, None must be a string in the view order_lines"]),
        ("label", "My View!", []),
        (
            "sql_table_name",
            None,
            ["The sql_table_name property, None must be a string in the view order_lines"],
        ),
        (
            "sql_table_name",
            1,
            ["The sql_table_name property, 1 must be a string in the view order_lines"],
        ),
        ("sql_table_name", "test", []),
        (
            "derived_table",
            "test",
            [
                (
                    "Warning: View order_lines has both sql_table_name and derived_table defined, "
                    "derived_table will be used"
                ),
                "The derived_table property, test must be a dictionary in the view order_lines",
            ],
        ),
        (
            "derived_table",
            {"test": "fake"},
            [
                (
                    "Warning: View order_lines has both sql_table_name and derived_table defined, "
                    "derived_table will be used"
                ),
                "Derived table in view order_lines is missing the sql property, this view will not work",
            ],
        ),
        (
            "derived_table",
            {"sql": "select * from this"},
            [
                (
                    "Warning: View order_lines has both sql_table_name and derived_table defined, "
                    "derived_table will be used"
                ),
            ],
        ),
        (
            "default_date",
            1,
            ["The default_date property, 1 must be a string in the view order_lines"],
        ),
        (
            "default_date",
            "fake",
            ["Default date fake in view order_lines is not joinable to the view order_lines"],
        ),
        ("row_label", None, ["The row_label property, None must be a string in the view order_lines"]),
        ("row_label", "Hello", []),
        ("sets", None, ["The sets property, None must be a list in the view order_lines"]),
        ("sets", ["test"], ["Set test in view order_lines must be a dictionary"]),
        (
            "sets",
            [{"name": "test_set", "fields": 1}],
            ["The fields property, 1 must be a list in the Set test_set"],
        ),
        (
            "sets",
            [{"name": "test_set", "fields": ["random_fake"]}],
            [
                (
                    "In the Set test_set Field random_fake not found in view order_lines, please "
                    "check that this field exists AND that you have access to it. \n"
                    "\n"
                    "If this is a dimension group specify the group parameter, if not already "
                    "specified, for example, with a dimension group named 'order' with "
                    "timeframes: [raw, date, month] specify 'order_raw' or 'order_date' or "
                    "'order_month'"
                ),
            ],
        ),
        ("sets", [{"name": "test_set", "fields": ["order_id"]}], []),
        ("always_filter", None, ["The always_filter property, None must be a list in the view order_lines"]),
        ("always_filter", [1], ["Always filter 1 in view order_lines must be a dictionary"]),
        (
            "always_filter",
            [{"name": "test"}],
            [
                "Always filter in View order_lines is missing the required field property",
                "Always filter in View order_lines is missing the required value property",
            ],
        ),
        (
            "always_filter",
            [{"field": "order_id"}],
            ["Always filter in View order_lines is missing the required value property"],
        ),
        (
            "always_filter",
            [{"field": "test", "value": "=1"}],
            [
                "Always filter in View order_lines is referencing a field, order_lines.test that does not"
                " exist"
            ],
        ),
        (
            "always_filter",
            [{"field": "order_id", "value": 2}],
            [
                "Always filter in View order_lines has an invalid value property. Valid values can be found"
                " here in the docs: https://docs.zenlytic.com/docs/data_modeling/field_filter"
            ],
        ),
        ("always_filter", [{"field": "order_id", "value": ""}], []),
        ("always_filter", [{"field": "order_id", "value": "-Paid"}], []),
        (
            "access_filters",
            None,
            [
                "The view order_lines has an access filter, None that is incorrectly specified as a when"
                " it should be a list, to specify it correctly check the documentation for access filters"
                " at https://docs.zenlytic.com/docs/data_modeling/access_grants#access-filters"
            ],
        ),
        ("access_filters", [1], ["Access filter 1 in view order_lines must be a dictionary"]),
        (
            "access_filters",
            [{"name": "test"}],
            [
                "Access filter in view order_lines is missing the required field property",
                "Access filter in view order_lines is missing the required user_attribute property",
            ],
        ),
        (
            "access_filters",
            [{"field": "test"}],
            [
                "Access filter in view order_lines is referencing a field, test that does not exist",
                "Access filter in view order_lines is missing the required user_attribute property",
            ],
        ),
        (
            "access_filters",
            [{"field": "test", "user_attribute": "test"}],
            ["Access filter in view order_lines is referencing a field, test that does not exist"],
        ),
        ("access_filters", [{"field": "order_lines.order_id", "user_attribute": "orders"}], []),
        (
            "access_grants",
            None,
            [
                "Property access_grants is present on View order_lines, but it is not a valid property. Did"
                " you mean required_access_grants?"
            ],
        ),
        (
            "required_access_grants",
            None,
            ["The required_access_grants property, None must be a list in view order_lines"],
        ),
        (
            "required_access_grants",
            [1],
            [
                "The access grant reference 1 in the required_access_grants property must be a"
                " string in view order_lines"
            ],
        ),
        (
            "required_access_grants",
            [{"name": "test"}],
            [
                "The access grant reference {'name': 'test'} in the "
                "required_access_grants property must be a string in view order_lines"
            ],
        ),
        (
            "required_access_grants",
            ["test"],
            [
                "The access grant test in the required_access_grants property does not exist "
                "in model test_model"
            ],
        ),
        ("required_access_grants", ["test_access_grant_department_customers"], []),
        (
            "event_dimension",
            None,
            ["The event_dimension property, None must be a string in the view order_lines"],
        ),
        (
            "event_dimension",
            "fake",
            ["The event_dimension property, order_lines.fake in the view order_lines is not a valid field"],
        ),
        ("event_dimension", "order_id", []),
        ("event_name", None, ["The event_name property, None must be a string in the view order_lines"]),
        ("event_name", "Hello", []),
        ("identifiers", None, ["The identifiers property, None must be a list in the view order_lines"]),
        ("identifiers", [], []),
        ("identifiers", [1], ["Identifier 1 in view order_lines must be a dictionary"]),
        (
            "identifiers",
            [{"field": "test"}],
            ["Identifier in view order_lines is missing the required name property"],
        ),
        (
            "identifiers",
            [{"name": "test"}],
            ["Identifier test in view order_lines is missing the required type property"],
        ),
        (
            "identifiers",
            [{"name": "test", "type": "primary"}],
            [
                "Could not find field test referenced in identifier test in view order_lines. Use the sql"
                " property to reference a different field in the view"
            ],
        ),
        (
            "identifiers",
            [{"name": -2, "type": "primary", "sql": "test"}],
            ["The name property, -2 in the identifier in view order_lines must be a string"],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "secondary", "sql": "test"}],
            [
                "The type property, secondary in the identifier customers in view order_lines must be one"
                " of ['primary', 'foreign', 'join']"
            ],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "primary", "sql": "${test}"}],
            ["Could not find field test referenced in identifier customers in view order_lines"],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "primary", "sql": "${order_id}"}],
            [],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"}],
            [],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "primary", "sql": "${order_id}"},
                {"name": "customers", "type": "foreign", "sql": "${order_line_id}"},
            ],
            ["Duplicate identifier name customers in view order_lines"],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"},
                {"name": "accounts", "type": "foreign", "sql": "${TABLE}.account_id"},
                {
                    "name": "customers_composite",
                    "type": "foreign",
                    "identifiers": [{"name": "customers"}],
                },
            ],
            [
                "The identifiers property on a composite key customers_composite  in view order_lines is only"
                " allowed for type: primary, not type: foreign"
            ],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"},
                {"name": "accounts", "type": "foreign", "sql": "${TABLE}.account_id"},
                {"name": "customers_composite", "type": "foreign", "identifiers": "customers"},
            ],
            [
                "The identifiers property, customers must be a list in the identifier customers_composite in"
                " view order_lines"
            ],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"},
                {"name": "accounts", "type": "foreign", "sql": "${TABLE}.account_id"},
                {
                    "name": "customers_composite",
                    "type": "primary",
                    "identifiers": ["customers"],
                },
            ],
            [
                (
                    "Identifier customers in the identifiers property of the identifier customers_composite"
                    " in view order_lines must be a dictionary"
                ),
            ],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"},
                {"name": "accounts", "type": "foreign", "sql": "${TABLE}.account_id"},
                {
                    "name": "customers_composite",
                    "type": "primary",
                    "identifiers": [{"name": "customers"}, {"name": "fake"}],
                },
            ],
            [
                "Reference to identifier fake in the composite key of identifier customers_composite in view"
                " order_lines does not exist"
            ],
        ),
        (
            "identifiers",
            [
                {"name": "customers", "type": "foreign", "sql": "${TABLE}.order_id"},
                {"name": "accounts", "type": "foreign", "sql": "${TABLE}.account_id"},
                {
                    "name": "customers_composite",
                    "type": "primary",
                    "identifiers": [{"name": "customers"}, {"name": "accounts"}],
                },
            ],
            [],
        ),
        (
            "identifiers",
            [{"name": "custom_join", "type": "join"}],
            [
                "Identifier custom_join in view order_lines is missing the required "
                "relationship property for the type: join. Options are: "
                "['many_to_one', 'one_to_one', 'one_to_many', 'many_to_many']"
            ],
        ),
        (
            "identifiers",
            [{"name": "custom_join", "type": "join", "relationship": "many_to_one"}],
            [
                "Identifier custom_join in view order_lines is missing the required sql_on property for the"
                " type: join"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "relationship": "many_to_one",
                    "sql_on": "${discounts.order_id}=${order_lines.order_id}",
                }
            ],
            [
                "Identifier custom_join in view order_lines is missing the required reference property for"
                " the type: join"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "relationship": "many_to_one",
                    "reference": "fake",
                    "sql_on": "${discounts.order_id}=${order_lines.order_id}",
                }
            ],
            [
                (
                    "The reference property, fake in the identifier custom_join in view order_lines is not a"
                    " valid view"
                ),
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "reference": "discounts",
                    "join_type": "left_outer",
                    "relationship": "many_one",
                    "sql_on": "${discounts.order_id}=${order_lines.order_id}",
                }
            ],
            [
                "Identifier custom_join in view order_lines has an invalid relationship "
                "property. Options are: ['many_to_one', 'one_to_one', 'one_to_many', "
                "'many_to_many']"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "reference": "discounts",
                    "join_type": "left",
                    "relationship": "many_to_one",
                    "sql_on": "${discounts.order_id}=${order_lines.order_id}",
                }
            ],
            [
                "The join_type property, left in the identifier custom_join in view "
                "order_lines must be one of ['left_outer', 'inner', 'full_outer', 'cross']"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "reference": "discounts",
                    "relationship": "many_to_one",
                    "sql_on": "${fake.order_id}=${order_lines.order_id}",
                }
            ],
            ["Could not find view fake in join between order_lines and discounts"],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "reference": "discounts",
                    "relationship": "many_to_one",
                    "sql_on": "${discounts.fake}=${order_lines.order_id}",
                }
            ],
            [
                "Could not find field fake in join between order_lines and discounts "
                "referencing view discounts"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "custom_join",
                    "type": "join",
                    "reference": "discounts",
                    "relationship": "many_to_one",
                    "sql_on": "${discounts.order_id}=${order_lines.order_id}",
                }
            ],
            [],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "primary", "sql": "${order_id}", "join_as": -2}],
            ["The join_as property, -2 in the identifier customers in view order_lines must be a string"],
        ),
        (
            "identifiers",
            [
                {
                    "name": "customers",
                    "type": "primary",
                    "sql": "${order_id}",
                    "join_as": "test",
                    "join_as_label": None,
                }
            ],
            [
                "The join_as_label property, None in the identifier customers in view order_lines must be a"
                " string"
            ],
        ),
        (
            "identifiers",
            [
                {
                    "name": "customers",
                    "type": "primary",
                    "sql": "${order_id}",
                    "join_as": "test",
                    "join_as_field_prefix": None,
                }
            ],
            [
                "The join_as_field_prefix property, None in the identifier customers in view order_lines must"
                " be a string"
            ],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "primary", "sql": "${order_id}", "random_arg": "testing"}],
            [
                "Property random_arg is present on Identifier customers, but it is not a valid property. in"
                " view order_lines"
            ],
        ),
        (
            "identifiers",
            [{"name": "customers", "type": "primary", "sql": "${order_id}", "join_as": "test"}],
            [],
        ),
        (
            "fields_for_analysis",
            "marketing_channel",
            ["The fields_for_analysis property, marketing_channel must be a list in the view order_lines"],
        ),
        (
            "extra",
            [],
            ["View order_lines has an invalid extra []. The extra must be a dictionary."],
        ),
        ("extra", {"random": "key"}, []),
        (
            "hidden",
            "yes",
            [
                "View order_lines has an invalid hidden value of yes. hidden must"
                " be a boolean (true or false)."
            ],
        ),
    ],
)
def test_validation_with_replaced_view_properties(connection, name, value, errors):
    project = connection.project
    view = _get_view_by_name(project, "order_lines")
    view[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[view])

    print(response)
    assert [e["message"] for e in response] == errors


@pytest.mark.validation
@pytest.mark.parametrize(
    "field_name,property_name,value,errors",
    [
        (
            "order_line_id",
            "primary_key",
            "__POP__",
            [
                "Warning: The view order_lines does not have a primary key, specify one using the tag"
                " primary_key: true"
            ],
        ),
        (
            None,
            "__ADD__",
            {
                "name": "order_line_id",
                "field_type": "dimension",
                "type": "string",
                "sql": "${TABLE}.order_line_id",
            },
            [
                (
                    "Multiple fields found for the name order_line_id, in view order_lines - "
                    "those fields were ['order_lines.order_line_id', "
                    "'order_lines.order_line_id']\n"
                    "\n"
                    "Please specify a view name like this: 'view_name.field_name' \n"
                    "\n"
                    "or change the names of the fields to ensure uniqueness"
                ),
                "Duplicate field names in view order_lines: order_line_id",
            ],
        ),
        (
            None,
            "__ADD__",
            {"name": "date", "field_type": "dimension", "type": "string", "sql": "${TABLE}.date"},
            ["Field name: date in view order_lines is a reserved word and cannot be used as a field name."],
        ),
        (
            "parent_channel",
            "name",
            "h@#ffw",
            [
                "Field name: h@#ffw is invalid. Please reference the naming conventions (only letters,"
                " numbers, or underscores)"
            ],
        ),
        (
            "parent_channel",
            "name",
            "__POP__",
            [
                "Field missing required key 'name' The field passed was {'field_type': "
                "'dimension', 'type': 'string', 'sql': \"CASE\\n--- parent "
                "channel\\nWHEN ${channel} ilike '%social%' then 'Social'\\nELSE 'Not "
                "Social'\\nEND\\n\"} in the view order_lines in the model test_model"
            ],
        ),
        (
            "parent_channel",
            "field_type",
            "__POP__",
            [
                "Field 'parent_channel' missing required key 'field_type' The field passed was {'name':"
                " 'parent_channel', 'type': 'string', 'sql': \"CASE\\n--- parent channel\\nWHEN ${channel}"
                " ilike '%social%' then 'Social'\\nELSE 'Not Social'\\nEND\\n\"} in the view order_lines in"
                " the model test_model"
            ],
        ),
        (
            "parent_channel",
            "field_type",
            "metric",
            [
                "Field parent_channel in view order_lines has an invalid field_type metric. Valid field_types"
                " are: ['dimension', 'dimension_group', 'measure']"
            ],
        ),
        (
            "parent_channel",
            "type",
            "__POP__",
            [
                "Field parent_channel in view order_lines is missing the required key 'type'.",
                (
                    "Field parent_channel in view order_lines has an invalid type None. Valid "
                    "types for dimensions are: ['string', 'yesno', 'number', 'tier']"
                ),
            ],
        ),
        (
            "parent_channel",
            "type",
            "count",
            [
                "Field parent_channel in view order_lines has an invalid type count. Valid "
                "types for dimensions are: ['string', 'yesno', 'number', 'tier']"
            ],
        ),
        (
            "order",
            "type",
            "yesno",
            [
                (
                    "Could not find field order_date in join between order_lines and "
                    "country_detail referencing view order_lines"
                ),
                (
                    "Could not find field order_date in join between country_detail and "
                    "order_lines referencing view order_lines"
                ),
                (
                    "Canon date order_lines.order is not of field_type: dimension_group and type: "
                    "time in field avg_rainfall in view country_detail"
                ),
                "Default date order is not of field_type: dimension_group and type: time in view order_lines",
                (
                    "Field order in view order_lines has an invalid type yesno. Valid types for "
                    "dimension groups are: ['time', 'duration']"
                ),
            ],
        ),
        (
            "total_item_costs",
            "type",
            "time",
            [
                (
                    "Field total_item_costs in view order_lines has an invalid type time. Valid "
                    "types for measures are: ['count', 'count_distinct', 'sum', 'sum_distinct', "
                    "'average', 'average_distinct', 'median', 'max', 'min', 'number', "
                    "'cumulative']"
                ),
            ],
        ),
        (
            "parent_channel",
            "label",
            -1,
            ["Field parent_channel in view order_lines has an invalid label -1. label must be a string."],
        ),
        (
            "parent_channel",
            "group_label",
            None,
            [
                "Field parent_channel in view order_lines has an invalid group_label None. group_label"
                " must be a string."
            ],
        ),
        (
            "parent_channel",
            "hidden",
            "yes",
            [
                "Field parent_channel in view order_lines has an invalid hidden value of yes. hidden must"
                " be a boolean (true or false)."
            ],
        ),
        (
            "parent_channel",
            "description",
            1,
            [
                "Field parent_channel in view order_lines has an invalid description 1. description must"
                " be a string."
            ],
        ),
        (
            "parent_channel",
            "zoe_description",
            None,
            [
                "Field parent_channel in view order_lines has an invalid zoe_description None. "
                "zoe_description must be a string."
            ],
        ),
        (
            "total_item_costs",
            "value_format_name",
            None,
            [
                "Field total_item_costs in view order_lines has an invalid value_format_name None. "
                "Valid value_format_names are: ['decimal_0', 'decimal_1', 'decimal_2', "
                "'decimal_pct_0', 'decimal_pct_1', 'decimal_pct_2', 'percent_0', 'percent_1', "
                "'percent_2', 'eur', 'eur_0', 'eur_1', 'eur_2', 'usd', 'usd_0', 'usd_1', "
                "'usd_2', 'string']"
            ],
        ),
        (
            "total_item_costs",
            "value_format_name",
            "aus",
            [
                "Field total_item_costs in view order_lines has an invalid value_format_name aus. "
                "Valid value_format_names are: ['decimal_0', 'decimal_1', 'decimal_2', "
                "'decimal_pct_0', 'decimal_pct_1', 'decimal_pct_2', 'percent_0', 'percent_1', "
                "'percent_2', 'eur', 'eur_0', 'eur_1', 'eur_2', 'usd', 'usd_0', 'usd_1', "
                "'usd_2', 'string']"
            ],
        ),
        (
            "total_item_costs",
            "value_format_name",
            "usd_0",
            [],
        ),
        (
            "total_item_costs",
            "synonyms",
            "cogs",
            [
                "Field total_item_costs in view order_lines has an invalid synonyms cogs. "
                "synonyms must be a list of strings."
            ],
        ),
        (
            "total_item_costs",
            "synonyms",
            [-3],
            [
                "Field total_item_costs in view order_lines has an invalid synonym -3. The "
                "synonym must be a string."
            ],
        ),
        (
            "total_item_costs",
            "filters",
            {"field": "order_id", "value": 1},
            [
                "Field total_item_costs in view order_lines has an invalid filters {'field': "
                "'order_id', 'value': 1}. The filters must be a list of dictionaries."
            ],
        ),
        (
            "total_item_costs",
            "filters",
            ["order_id"],
            [
                "Field total_item_costs in view order_lines has an invalid filter order_id. "
                "filter must be a dictionary."
            ],
        ),
        (
            "total_item_costs",
            "filters",
            [{"field": "order_id", "value": ""}],  # This is valid, but will be ignored
            [],
        ),
        (
            "total_item_costs",
            "filters",
            [{"field": "order_id", "value": ">1", "random": "key"}],
            [
                "Property random is present on Field Filter in field total_item_costs in view "
                "order_lines, but it is not a valid property."
            ],
        ),
        ("total_item_costs", "filters", [{"field": "order_id", "value": 1}], []),
        (
            "total_item_costs",
            "extra",
            [],
            [
                "Field total_item_costs in view order_lines has an invalid extra []. The "
                "extra must be a dictionary."
            ],
        ),
        ("total_item_costs", "extra", {"random": "key"}, []),
        (
            "parent_channel",
            "primary_key",
            "yes",
            [
                (
                    "Field parent_channel in view order_lines has an invalid primary_key yes. "
                    "primary_key must be a boolean (true or false)."
                ),
                (
                    "Multiple primary keys found in view order_lines: order_line_id, "
                    "parent_channel. Only one primary key is allowed"
                ),
            ],
        ),
        (
            "parent_channel",
            "primary_key",
            True,
            [
                "Multiple primary keys found in view order_lines: order_line_id, "
                "parent_channel. Only one primary key is allowed"
            ],
        ),
        (
            "order",
            "primary_key",
            True,
            [
                "Multiple primary keys found in view order_lines: order, order_line_id"
                ". Only one primary key is allowed"
            ],
        ),
        (
            "total_item_costs",
            "primary_key",
            True,
            [
                (
                    "Field total_item_costs in view order_lines has an invalid primary_key True. "
                    "primary_key is not a valid property for measures."
                ),
                (
                    "Property primary_key is present on Field total_item_costs in view "
                    "order_lines, but it is not a valid property."
                ),
            ],
        ),
        (
            "total_item_costs",
            "type",
            "string",
            [
                "Field total_item_costs in view order_lines has an invalid type string. Valid types for"
                " measures are: ['count', 'count_distinct', 'sum', 'sum_distinct', 'average',"
                " 'average_distinct', 'median', 'max', 'min', 'number', 'cumulative']"
            ],
        ),
        (
            "parent_channel",
            "type",
            "count",
            [
                "Field parent_channel in view order_lines has an invalid type count. Valid types for"
                " dimensions are: ['string', 'yesno', 'number', 'tier']"
            ],
        ),
        (
            "order",
            "type",
            "number",
            [
                (
                    "Could not find field order_date in join between order_lines and "
                    "country_detail referencing view order_lines"
                ),
                (
                    "Could not find field order_date in join between country_detail and "
                    "order_lines referencing view order_lines"
                ),
                (
                    "Canon date order_lines.order is not of field_type: dimension_group and type: "
                    "time in field avg_rainfall in view country_detail"
                ),
                "Default date order is not of field_type: dimension_group and type: time in view order_lines",
                (
                    "Field order in view order_lines has an invalid type number. Valid types for dimension"
                    " groups are: ['time', 'duration']"
                ),
            ],
        ),
        (
            "order",
            "intervals",
            ["seconds"],
            [
                (
                    "Field order in view order_lines is of type time, but has property intervals when it"
                    " should have property timeframes"
                ),
            ],
        ),
        (
            "order",
            "timeframes",
            ["timestamp"],
            [
                (
                    "Could not find field order_date in join between order_lines and "
                    "country_detail referencing view order_lines"
                ),
                (
                    "Could not find field order_date in join between country_detail and "
                    "order_lines referencing view order_lines"
                ),
                (
                    "Field order in view order_lines is of type time and has timeframe value of 'timestamp'"
                    " which is not a valid timeframes (valid timeframes are ['raw', 'time', 'second',"
                    " 'minute', 'hour', 'date', 'week', 'month', 'quarter', 'year', 'week_index',"
                    " 'week_of_year', 'week_of_month', 'month_of_year', 'month_of_year_index', 'month_name',"
                    " 'month_index', 'quarter_of_year', 'hour_of_day', 'day_of_week', 'day_of_month',"
                    " 'day_of_year'])"
                ),
            ],
        ),
        (
            "waiting",
            "sql",
            "${TABLE}.mycol",
            [
                "Field waiting in view order_lines is a dimension group of type duration, but "
                "has a sql property. Dimension groups of type duration must not have a sql "
                "property (just sql_start and sql_end)."
            ],
        ),
        (
            "waiting",
            "timeframes",
            ["time"],
            [
                "Field waiting in view order_lines is of type duration, but has property timeframes when it "
                "should have property intervals"
            ],
        ),
        (
            "waiting",
            "intervals",
            ["time"],
            [
                "Field waiting in view order_lines is of type duration and has interval value of 'time' which"
                " is not a valid interval (valid intervals are ['second', 'minute', 'hour', 'day', 'week',"
                " 'month', 'quarter', 'year'])"
            ],
        ),
        (
            "parent_channel",
            "required_access_grants",
            None,
            [
                "The required_access_grants property, None must be a list in field parent_channel in view"
                " order_lines"
            ],
        ),
        (
            "parent_channel",
            "required_access_grants",
            [1],
            [
                "The access grant reference 1 in the required_access_grants property must be a"
                " string in field parent_channel in view order_lines"
            ],
        ),
        (
            "parent_channel",
            "required_access_grants",
            [{"name": "test"}],
            [
                "The access grant reference {'name': 'test'} in the "
                "required_access_grants property must be a string in field parent_channel in view order_lines"
            ],
        ),
        (
            "parent_channel",
            "required_access_grants",
            ["test"],
            [
                "The access grant test in the required_access_grants property does not exist "
                "in model test_model"
            ],
        ),
        ("parent_channel", "required_access_grants", ["test_access_grant_department_customers"], []),
        (
            "total_item_costs",
            "canon_date",
            True,
            [
                "Field total_item_costs in view order_lines has an invalid canon_date True. "
                "canon_date must be a string."
            ],
        ),
        (
            "total_item_costs",
            "canon_date",
            "parent_channel",
            [
                "Canon date order_lines.parent_channel is not of field_type: dimension_group "
                "and type: time in field total_item_costs in view order_lines"
            ],
        ),
        ("total_item_costs", "canon_date", "orders.order", []),
        ("total_item_costs", "canon_date", "${order}", []),
        (
            "total_item_costs",
            "sql",
            None,
            ["Field total_item_costs in view order_lines has an invalid sql None. sql must be a string."],
        ),
        (
            "total_item_costs",
            "sql",
            1,
            ["Field total_item_costs in view order_lines has an invalid sql 1. sql must be a string."],
        ),
        ("total_item_costs", "sql", "1", []),
        (
            "total_item_costs",
            "sql",
            "${TABL}.mycol",
            ["Could not locate reference tabl in field total_item_costs in view order_lines"],
        ),
        ("total_item_costs", "sql", "${TABLE}.mycol", []),
        ("total_item_costs", "sql", "${order_date}", []),
        (
            "parent_channel",
            "sql",
            None,
            [
                "Field parent_channel in view order_lines has an invalid sql None. sql must be a string. The"
                " sql property must be present for dimensions."
            ],
        ),
        (
            "order",
            "sql",
            None,
            [
                "Field order in view order_lines is a dimension group of type time, but does "
                "not have a sql valid property. Dimension groups of type time must have a sql "
                "property and that property must be a string."
            ],
        ),
        (
            "waiting",
            "sql_start",
            None,
            [
                "Field waiting in view order_lines has an invalid sql_start None. sql_start must be a string."
                " The sql_start property must be present for dimension groups of type duration."
            ],
        ),
        (
            "waiting",
            "sql_end",
            None,
            [
                "Field waiting in view order_lines has an invalid sql_end None. sql_end must be a string. The"
                " sql_end property must be present for dimension groups of type duration."
            ],
        ),
        (
            "waiting",
            "sql_end",
            "${TABL}.mycol",
            ["Could not locate reference tabl in field waiting in view order_lines"],
        ),
        ("waiting", "sql_end", "${TABLE}.mycol", []),
        ("waiting", "sql_end", "${order_date}", []),
        (
            "order",
            "convert_tz",
            None,
            [
                "Field order in view order_lines has an invalid convert_tz None. "
                "convert_tz must be a boolean (true or false)."
            ],
        ),
        (
            "order",
            "convert_timezone",
            -3,
            [
                "Field order in view order_lines has an invalid convert_timezone -3. "
                "convert_timezone must be a boolean (true or false)."
            ],
        ),
        (
            "order",
            "datatype",
            "time",
            [
                "Field order in view order_lines has an invalid datatype time. Valid "
                "datatypes for time dimension groups are: ['timestamp', 'date', 'datetime']"
            ],
        ),
        (
            "total_item_costs",
            "type",
            "cumulative",
            [
                (
                    "Field total_item_costs in view order_lines is a cumulative metric (measure), "
                    "but does not have a measure property."
                ),
                (
                    "Field total_item_costs in view order_lines is a cumulative metric (measure), "
                    "but the measure property None is unreachable."
                ),
            ],
        ),
        (
            "total_item_costs",
            ("type", "measure"),
            ("cumulative", None),
            [
                (
                    "Field total_item_costs in view order_lines has an invalid measure None. "
                    "measure must be a string."
                ),
                (
                    "Field total_item_costs in view order_lines is a cumulative metric (measure), "
                    "but the measure property None is unreachable."
                ),
            ],
        ),
        (
            "total_item_costs",
            ("type", "measure"),
            ("cumulative", "fake"),
            [
                (
                    "Field fake not found in view order_lines, please check that this field "
                    "exists AND that you have access to it. \n"
                    "\n"
                    "If this is a dimension group specify the group parameter, if not already "
                    "specified, for example, with a dimension group named 'order' with "
                    "timeframes: [raw, date, month] specify 'order_raw' or 'order_date' or "
                    "'order_month' in the view order_lines"
                ),
                (
                    "Field total_item_costs in view order_lines is a cumulative metric (measure), "
                    "but the measure property fake is unreachable."
                ),
            ],
        ),
        (
            "total_item_costs",
            ("type", "measure"),
            ("cumulative", "parent_channel"),
            [
                "Field total_item_costs in view order_lines is a cumulative metric (measure), "
                "but the measure property parent_channel is not a "
                "measure."
            ],
        ),
        (
            "total_item_costs",
            ("type", "measure", "cumulative_where"),
            ("cumulative", "number_of_email_purchased_items", True),
            [
                "Field total_item_costs in view order_lines has an invalid cumulative_where "
                "True. cumulative_where must be a string."
            ],
        ),
        (
            "total_item_costs",
            ("type", "measure", "update_where_timeframe"),
            ("cumulative", "number_of_email_purchased_items", "hi"),
            [
                "Field total_item_costs in view order_lines has an invalid "
                "update_where_timeframe hi. update_where_timeframe must be a boolean (true or "
                "false)."
            ],
        ),
        ("total_item_costs", ("type", "measure"), ("cumulative", "number_of_email_purchased_items"), []),
        (
            "total_item_costs",
            "type",
            "sum_distinct",
            [
                "Field total_item_costs in view order_lines is a measure of type "
                "sum_distinct, but does not have a sql_distinct_key property."
            ],
        ),
        (
            "total_item_costs",
            "type",
            "average_distinct",
            [
                "Field total_item_costs in view order_lines is a measure of type "
                "average_distinct, but does not have a sql_distinct_key property."
            ],
        ),
        (
            "total_item_costs",
            ("type", "sql_distinct_key"),
            ("sum_distinct", None),
            [
                "Field total_item_costs in view order_lines is a measure of type "
                "sum_distinct, but does not have a sql_distinct_key property."
            ],
        ),
        (
            "total_item_costs",
            ("type", "sql_distinct_key"),
            ("sum_distinct", "${fake}"),
            [
                "Field total_item_costs in view order_lines has an invalid sql_distinct_key ${fake}. The"
                " field fake referenced in sql_distinct_key does not exist."
            ],
        ),
        ("total_item_costs", ("type", "sql_distinct_key"), ("sum_distinct", "${order_id}"), []),
        ("total_item_costs", ("type", "sql_distinct_key"), ("sum_distinct", "order_id"), []),
        (
            "total_item_costs",
            "non_additive_dimension",
            None,
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension None. non_additive_dimension must be a dictionary."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            2,
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension 2. non_additive_dimension must be a dictionary."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {},
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension {}. non_additive_dimension must have a 'name' "
                "property that references a type time dimension group."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "fake", "window_choice": "max"},
            [
                "Could not locate reference order_lines.fake in field total_item_costs in view order_lines",
                (
                    "Field total_item_costs in view order_lines has an invalid "
                    "non_additive_dimension. The field order_lines.fake referenced in "
                    "non_additive_dimension does not exist."
                ),
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "parent_channel", "window_choice": "max"},
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension. The field order_lines.parent_channel referenced in "
                "non_additive_dimension is not a valid dimension group with type time."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "top"},
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension. window_choice must be either 'max' or 'min'."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "max", "window_aware_of_query_dimensions": 2},
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension. window_aware_of_query_dimensions must be a boolean."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "max", "window_groupings": "account"},
            [
                "Field total_item_costs in view order_lines has an invalid "
                "non_additive_dimension. window_groupings must be a list."
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "max", "window_groupings": ["fake"]},
            [
                "Could not locate reference order_lines.fake in field total_item_costs in view order_lines",
                (
                    "Field total_item_costs in view order_lines has an invalid "
                    "non_additive_dimension. The field order_lines.fake "
                    "referenced in window_groupings does not exist."
                ),
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "max", "window_grouping": ["order_id"]},
            [
                "Property window_grouping is present on Non Additive Dimension in field "
                "total_item_costs in view order_lines, but it is not a valid property. Did "
                "you mean window_groupings?"
            ],
        ),
        (
            "total_item_costs",
            "non_additive_dimension",
            {"name": "order_raw", "window_choice": "max", "window_groupings": ["order_id"]},
            [],
        ),
        (
            "parent_channel",
            "tags",
            2,
            [
                "Field parent_channel in view order_lines has an invalid tags 2. tags must be "
                "a list of strings."
            ],
        ),
        (
            "parent_channel",
            "tags",
            [-2],
            [
                "Field parent_channel in view order_lines has an invalid tag -2. tags must be "
                "a list of strings."
            ],
        ),
        (
            "parent_channel",
            "drill_fields",
            2,
            [
                "Field parent_channel in view order_lines has an invalid drill_fields. "
                "drill_fields must be a list of strings."
            ],
        ),
        (
            "parent_channel",
            "drill_fields",
            ["fake"],
            [
                "Field order_lines.fake in drill_fields is unreachable in field "
                "parent_channel in view order_lines."
            ],
        ),
        ("parent_channel", "drill_fields", ["order_id"], []),
        (
            "parent_channel",
            "searchable",
            -1,
            [
                "Field parent_channel in view order_lines has an invalid searchable -1. "
                "searchable must be a boolean (true or false)."
            ],
        ),
        (
            "parent_channel",
            "type",
            "tier",
            [
                "Field parent_channel in view order_lines is of type tier, but does not have "
                "a tiers property. The tiers property is required for dimensions of type: "
                "tier."
            ],
        ),
        (
            "parent_channel",
            ("type", "tiers"),
            ("tier", ["top", "bottom"]),
            [
                (
                    "Field parent_channel in view order_lines has an invalid tier top. tiers must "
                    "be a list of integers."
                ),
                (
                    "Field parent_channel in view order_lines has an invalid tier bottom. tiers "
                    "must be a list of integers."
                ),
            ],
        ),
        ("parent_channel", ("type", "tiers"), ("tier", [0, 10, 20]), []),
        (
            "parent_channel",
            "link",
            -1,
            ["Field parent_channel in view order_lines has an invalid link -1. link must be a string."],
        ),
        ("parent_channel", "link", "https://google.com", []),
        (
            "total_item_costs",
            "is_merged_result",
            "yeah",
            [
                "Field total_item_costs in view order_lines has an invalid is_merged_result yeah. "
                "is_merged_result must be a boolean (true or false)."
            ],
        ),
        (
            "parent_channel",
            "random_key",
            "yay",
            [
                "Property random_key is present on Field parent_channel in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "parent_channel",
            "sql_distinct_key",
            "${order_line_id}",
            [
                "Property sql_distinct_key is present on Field parent_channel in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "parent_channel",
            "datatype",
            "timestamp",
            [
                "Property datatype is present on Field parent_channel in view order_lines, but "
                "it is not a valid property. Did you mean type?"
            ],
        ),
        (
            "total_item_costs",
            "random_key",
            "yay",
            [
                "Property random_key is present on Field total_item_costs in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "total_item_costs",
            "datatype",
            "timestamp",
            [
                "Property datatype is present on Field total_item_costs in view order_lines, but "
                "it is not a valid property. Did you mean type?"
            ],
        ),
        (
            "total_item_costs",
            "searchable",
            True,
            [
                "Property searchable is present on Field total_item_costs in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "order",
            "random_key",
            "yay",
            [
                "Property random_key is present on Field order in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "order",
            "sql_distinct_key",
            "${order_line_id}",
            [
                "Property sql_distinct_key is present on Field order in view order_lines, but "
                "it is not a valid property."
            ],
        ),
        (
            "order",
            "tiers",
            [0, 20, 100],
            [
                "Property tiers is present on Field order in view order_lines, but it is not "
                "a valid property. Did you mean timeframes?"
            ],
        ),
        (
            "parent_channel",
            "case",
            {
                "whens": [{"sql": "${TABLE}.product_name ilike '%sale%'", "label": "On sale"}],
                "else": "Not on sale",
            },
            [
                "Warning:: Field parent_channel in view order_lines is using a case "
                "statement, which is deprecated. Please use the sql property instead."
            ],
        ),
    ],
)
def test_validation_with_replaced_field_properties(connection, field_name, property_name, value, errors):
    project = connection.project
    view = _get_view_by_name(project, "order_lines")

    if property_name == "__ADD__":
        field = value
    else:
        field = _get_field_by_name(view, field_name)

    if value == "__POP__":
        field.pop(property_name)
    elif property_name == "__ADD__":
        view["fields"].append(value)
    elif isinstance(property_name, tuple):
        for p, v in zip(property_name, value):
            field[p] = v
    else:
        field[property_name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[view])

    print(response)
    assert [e["message"] for e in response] == errors
