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
            ["The access_grants property, None must be a list in the model test_model"],
        ),
        (
            "access_grants",
            [1],
            ["Each access_grant in the access_grants property must be a dictionary in the model test_model"],
        ),
        (
            "access_grants",
            [{"name": "test"}],
            ["Access Grant test missing required key user_attribute in the model test_model"],
        ),
        (
            "access_grants",
            [{"model": "test"}],
            ["Access Grant missing required key name in the model test_model"],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "test", "allowed_values": "test"}],
            ["The allowed_values property, test must be a list in the Access Grant test"],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "test", "allowed_values": [1, 2]}],
            ["All values in the allowed_values property must be strings in the Access Grant test"],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": None, "allowed_values": ["1", "2"]}],
            ["The user_attribute property, None must be a string in the Access Grant test"],
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
                "Property allowed_val is present on Access Grant test, but it is not a valid property. Did"
                " you mean allowed_values?"
            ],
        ),
        (
            "access_grants",
            [{"name": "test", "user_attribute": "products", "allowed_values": ["blush", "eyeliner"]}],
            [],
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
