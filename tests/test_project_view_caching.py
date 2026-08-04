import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException


def test_views_returns_cached_view_instances(fresh_project):
    first = fresh_project.views()
    second = fresh_project.views()
    assert len(first) > 0
    assert all(a is b for a, b in zip(first, second))


def test_get_view_returns_cached_view_instance(fresh_project):
    assert fresh_project.get_view("orders") is fresh_project.get_view("orders")


def test_get_view_matches_linear_scan_result_for_every_view(fresh_project):
    for view in fresh_project.views():
        assert fresh_project.get_view(view.name) is view


def test_get_view_unknown_view_raises(fresh_project):
    with pytest.raises(AccessDeniedOrDoesNotExistException):
        fresh_project.get_view("view_that_does_not_exist")


def test_get_view_respects_model_name_filter(fresh_project):
    view = fresh_project.get_view("orders", model_name="test_model")
    assert view.name == "orders"
    with pytest.raises(AccessDeniedOrDoesNotExistException):
        fresh_project.get_view("orders", model_name="new_model")


def test_set_user_invalidates_cached_views(fresh_project):
    fresh_project.set_user({"department": "sales"})
    assert fresh_project.get_view("orders").name == "orders"

    fresh_project.set_user({"department": "marketing"})
    with pytest.raises(AccessDeniedOrDoesNotExistException):
        fresh_project.get_view("orders")


def test_view_fields_expansion_flag_is_not_sticky_on_shared_instances(fresh_project):
    view = fresh_project.get_view("order_lines")

    expanded = view.fields(expand_dimension_groups=True)
    unexpanded = view.fields(expand_dimension_groups=False)

    expanded_ids = [f.id() for f in expanded]
    assert "order_lines.order_date" in expanded_ids
    assert "order_lines.order_month" in expanded_ids

    unexpanded_ids = [f.id() for f in unexpanded]
    assert "order_lines.order_date" not in unexpanded_ids
    # The unexpanded list keeps the dimension group as a single field
    assert sum(1 for f in unexpanded if f.name == "order") == 1
    assert sum(1 for f in expanded if f.name == "order") > 1


def test_dimension_group_binding_does_not_leak_between_lookups(fresh_project):
    # Field.equal binds a timeframe onto unexpanded dimension-group fields
    # (e.g. matching "order_date" sets dimension_group = "date"). With views
    # shared through the project's cached lookups, one caller's binding must
    # not poison another caller's lookup for a different timeframe.
    view = fresh_project.get_view("order_lines")
    assert any(f.equal("order_date") for f in view.fields())
    assert any(f.equal("order_week") for f in view.fields())


def test_field_mutation_invalidates_cached_views(fresh_project):
    fresh_project.get_view("orders")

    new_field = {
        "name": "cached_view_test_field",
        "field_type": "dimension",
        "type": "string",
        "sql": "${TABLE}.cached_view_test_col",
    }
    fresh_project.add_field(new_field, "orders")

    field_names = [f.name for f in fresh_project.get_view("orders").fields()]
    assert "cached_view_test_field" in field_names
