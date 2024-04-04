from copy import deepcopy

import pytest


def _get_view_by_name(project, view_name):
    for view in project._views:
        if view["name"] == view_name:
            return deepcopy(view)
    raise ValueError(f"View {view_name} not found in project views")


@pytest.mark.validation
def test_validation_with_no_replaced_objects(connection):
    project = connection.project
    response = project.validate_with_replaced_objects(replaced_objects=[])
    assert response == []


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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
                ),
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "products", "allowed_values": ["blush", "eyeliner"]}],
            [
                (
                    "The access grant test_access_grant_department_view in the "
                    "required_access_grants property does not exist in the model test_model"
                ),
                (
                    "The access grant test_access_grant_department_customers in the "
                    "required_access_grants property does not exist in the model test_model"
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
            {"myfield": {"fields": ["orders.sub_channel", "sessions.utm_campaign"]}},
            [],
        ),
        (
            "mappings",
            {
                "myfield": {
                    "fields": ["orders.sub_channel", "sessions.utm_campaign"],
                    "description": "This is a test",
                    "group_label": "Test",
                },
            },
            [],
        ),
    ],
)
def test_validation_with_replaced_model_properties(connection, name, value, errors):
    project = connection.project
    model = deepcopy(project._models[0])
    model[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[model])

    assert response == errors


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
            ["The required_access_grants property, None must be a list in the view order_lines"],
        ),
        (
            "required_access_grants",
            [1],
            [
                "The access grant reference 1 in the required_access_grants property must be a"
                " string in the view order_lines"
            ],
        ),
        (
            "required_access_grants",
            [{"name": "test"}],
            [
                "The access grant reference {'name': 'test'} in the "
                "required_access_grants property must be a string in the view order_lines"
            ],
        ),
        (
            "required_access_grants",
            ["test"],
            [
                "The access grant test in the required_access_grants property does not exist "
                "in the model test_model"
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
    ],
)
def test_validation_with_replaced_view_properties(connection, name, value, errors):
    project = connection.project
    view = _get_view_by_name(project, "order_lines")
    view[name] = value
    response = project.validate_with_replaced_objects(replaced_objects=[view])

    print(response)
    assert response == errors


# Primary key
# Duplicate fields
