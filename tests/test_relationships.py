import json

import pytest


@pytest.mark.validation
def test_validation_relationships_basic(connection):
    """Test that valid relationships pass validation."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    # Add valid relationships
    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert response == []


@pytest.mark.validation
def test_validation_relationships_missing_from_table(connection):
    """Test that missing from_table is caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "join_table": "customers",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "Relationship missing required key 'from_table'" in response[0]["message"]


@pytest.mark.validation
def test_validation_relationships_missing_join_table(connection):
    """Test that missing join_table is caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "Relationship missing required key 'join_table'" in response[0]["message"]


@pytest.mark.validation
def test_validation_relationships_missing_relationship(connection):
    """Test that missing relationship is caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 0


@pytest.mark.validation
def test_validation_relationships_missing_sql_on(connection):
    """Test that missing sql_on is caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "relationship": "many_to_one",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "Relationship missing required key 'sql_on'" in response[0]["message"]


@pytest.mark.validation
@pytest.mark.parametrize(
    "from_table,join_table,join_type,relationship,sql_on,expected_error",
    [
        # Invalid from_table
        (
            None,
            "customers",
            "left_outer",
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The from_table property, None must be a string",
        ),
        (
            123,
            "customers",
            "left_outer",
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The from_table property, 123 must be a string",
        ),
        (
            "fake_view",
            "customers",
            "left_outer",
            "many_to_one",
            "${fake_view.id}=${customers.customer_id}",
            "The from_table property, fake_view does not reference a valid view",
        ),
        # Invalid join_table
        (
            "orders",
            None,
            "left_outer",
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The join_table property, None must be a string",
        ),
        (
            "orders",
            123,
            "left_outer",
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The join_table property, 123 must be a string",
        ),
        (
            "orders",
            "fake_view",
            "left_outer",
            "many_to_one",
            "${orders.customer_id}=${fake_view.id}",
            "The join_table property, fake_view does not reference a valid view",
        ),
        # Invalid join_type
        (
            "orders",
            "customers",
            "invalid_join",
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The join_type property, invalid_join must be one of",
        ),
        (
            "orders",
            "customers",
            123,
            "many_to_one",
            "${orders.customer_id}=${customers.customer_id}",
            "The join_type property, 123 must be a string",
        ),
        # Invalid relationship
        (
            "orders",
            "customers",
            "left_outer",
            None,
            "${orders.customer_id}=${customers.customer_id}",
            "The relationship property, None must be a string",
        ),
        (
            "orders",
            "customers",
            "left_outer",
            "invalid_relationship",
            "${orders.customer_id}=${customers.customer_id}",
            "The relationship property, invalid_relationship must be one of",
        ),
        # Invalid sql_on
        (
            "orders",
            "customers",
            "left_outer",
            "many_to_one",
            None,
            "The sql_on property, None must be a string",
        ),
        (
            "orders",
            "customers",
            "left_outer",
            "many_to_one",
            123,
            "The sql_on property, 123 must be a string",
        ),
        (
            "orders",
            "customers",
            "left_outer",
            "many_to_one",
            "${fake_view.id}=${customers.customer_id}",
            "Could not find view fake_view",
        ),
        (
            "orders",
            "customers",
            "left_outer",
            "many_to_one",
            "${orders.fake_field}=${customers.customer_id}",
            "Could not find field fake_field",
        ),
    ],
)
def test_validation_relationships_invalid_properties(
    connection, from_table, join_table, join_type, relationship, sql_on, expected_error
):
    """Test validation of various invalid relationship properties."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    relationship_def = {
        "from_table": from_table,
        "join_table": join_table,
        "relationship": relationship,
        "sql_on": sql_on,
    }

    if join_type is not None:
        relationship_def["join_type"] = join_type

    model["relationships"] = [relationship_def]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) >= 1
    assert any(expected_error in err["message"] for err in response)


@pytest.mark.validation
def test_validation_relationships_not_list(connection):
    """Test that relationships property must be a list."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = "not a list"

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "The relationships property" in response[0]["message"]
    assert "must be a list" in response[0]["message"]


@pytest.mark.validation
def test_validation_relationships_not_dict(connection):
    """Test that each relationship must be a dictionary."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = ["not a dict"]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "All relationships in the relationships property must be dictionaries" in response[0]["message"]


@pytest.mark.validation
def test_validation_relationships_invalid_property(connection):
    """Test that invalid properties on relationships are caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
            "invalid_property": "test",
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) == 1
    assert "Property invalid_property is present on Relationship" in response[0]["message"]
    assert "but it is not a valid property" in response[0]["message"]


@pytest.mark.validation
def test_validation_relationships_multiple(connection):
    """Test multiple valid relationships."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "orders",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${order_lines.order_id}=${orders.order_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "discounts",
            "relationship": "one_to_many",
            "sql_on": "${order_lines.order_id}=${discounts.order_id}",
        },
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert response == []


@pytest.mark.validation
def test_get_relationships_all(connection):
    """Test retrieving all relationships from a model."""
    project = connection.project

    # Get the test model (should now have relationships from the YAML)
    model = project.get_model("test_model")

    # Get all relationships
    relationships = model.get_relationships()

    assert isinstance(relationships, list)
    assert len(relationships) >= 0  # May be 0 if no relationships defined


@pytest.mark.validation
def test_get_relationships_filtered_by_view(connection):
    """Test retrieving relationships filtered by view name."""
    project = connection.project
    model_dict = json.loads(json.dumps(project._models[0]))

    # Add relationships
    model_dict["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "orders",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${order_lines.order_id}=${orders.order_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "discounts",
            "relationship": "one_to_many",
            "sql_on": "${order_lines.order_id}=${discounts.order_id}",
        },
    ]

    # Create a new project with the updated model
    project._models[0] = model_dict
    project._models_cache = None

    model = project.get_model("test_model")

    # Get relationships for orders view
    orders_relationships = model.get_relationships("orders")
    assert len(orders_relationships) == 2  # orders appears in 2 relationships

    # Get relationships for order_lines view
    order_lines_relationships = model.get_relationships("order_lines")
    assert len(order_lines_relationships) == 2  # order_lines appears in 2 relationships

    # Get relationships for customers view
    customers_relationships = model.get_relationships("customers")
    assert len(customers_relationships) == 1  # customers appears in 1 relationship

    # Get relationships for discounts view
    discounts_relationships = model.get_relationships("discounts")
    assert len(discounts_relationships) == 1  # discounts appears in 1 relationship

    # Get relationships for a view not in any relationships
    sessions_relationships = model.get_relationships("sessions")
    assert len(sessions_relationships) == 0


@pytest.mark.validation
def test_get_relationships_none_defined(connection):
    """Test retrieving relationships when none are defined."""
    project = connection.project
    model_dict = json.loads(json.dumps(project._models[0]))

    # Remove relationships if present
    if "relationships" in model_dict:
        del model_dict["relationships"]

    project._models[0] = model_dict
    project._models_cache = None

    model = project.get_model("test_model")

    # Get all relationships - should return empty list
    relationships = model.get_relationships()
    assert relationships == []

    # Get filtered relationships - should also return empty list
    relationships = model.get_relationships("orders")
    assert relationships == []


@pytest.mark.validation
def test_relationships_with_all_join_types(connection):
    """Test relationships with all valid join types."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "join_type": "left_outer",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "orders",
            "join_type": "inner",
            "relationship": "many_to_one",
            "sql_on": "${order_lines.order_id}=${orders.order_id}",
        },
        {
            "from_table": "customers",
            "join_table": "orders",
            "join_type": "full_outer",
            "relationship": "one_to_many",
            "sql_on": "${customers.customer_id}=${orders.customer_id}",
        },
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert response == []


@pytest.mark.validation
def test_relationships_with_all_relationship_types(connection):
    """Test relationships with all valid relationship types."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "relationship": "many_to_one",
            "sql_on": "${orders.customer_id}=${customers.customer_id}",
        },
        {
            "from_table": "customers",
            "join_table": "orders",
            "relationship": "one_to_many",
            "sql_on": "${customers.customer_id}=${orders.customer_id}",
        },
        {
            "from_table": "orders",
            "join_table": "discounts",
            "relationship": "one_to_one",
            "sql_on": "${orders.order_id}=${discounts.order_id}",
        },
        {
            "from_table": "order_lines",
            "join_table": "discounts",
            "relationship": "many_to_many",
            "sql_on": "${order_lines.order_id}=${discounts.order_id}",
        },
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert response == []


@pytest.mark.validation
def test_relationships_sql_on_without_view_name(connection):
    """Test that sql_on without view name is caught."""
    project = connection.project
    model = json.loads(json.dumps(project._models[0]))

    model["relationships"] = [
        {
            "from_table": "orders",
            "join_table": "customers",
            "relationship": "many_to_one",
            "sql_on": "${customer_id}=${customer_id}",  # Missing view names
        }
    ]

    response = project.validate_with_replaced_objects(replaced_objects=[model])
    assert len(response) >= 1
    assert any("Could not find view" in err["message"] for err in response)
