import pytest

from metrics_layer.core.model.project import Project


def _view(name, model_name):
    return {
        "version": 1,
        "type": "view",
        "name": name,
        "model_name": model_name,
        "sql_table_name": f"analytics.{name}",
        "default_date": "created",
        "identifiers": [{"name": f"{name}_id", "type": "primary", "sql": "${id}"}],
        "fields": [
            {
                "name": "id",
                "field_type": "dimension",
                "type": "string",
                "sql": "${TABLE}.id",
                "primary_key": True,
            },
            {
                "name": "created",
                "field_type": "dimension_group",
                "type": "time",
                "timeframes": ["raw", "date", "month"],
                "sql": "${TABLE}.created_at",
            },
            {"name": "total", "field_type": "measure", "type": "sum", "sql": "${TABLE}.amount"},
        ],
    }


def _project():
    models = [
        {"version": 1, "type": "model", "name": "model_a", "connection": "warehouse"},
        {"version": 1, "type": "model", "name": "model_b", "connection": "warehouse"},
    ]
    views = [_view("a_orders", "model_a"), _view("b_events", "model_b")]
    return Project(models=models, views=views, connection_lookup={"warehouse": "SNOWFLAKE"})


@pytest.mark.project
def test_join_graphs_do_not_depend_on_which_model_was_asked_first():
    alone = _project().get_field("b_events.total").join_graphs()

    project = _project()
    project.get_field("a_orders.total").join_graphs()
    after_other_model = project.get_field("b_events.total").join_graphs()

    assert any("merged_result" in jg for jg in alone)
    assert after_other_model == alone


@pytest.mark.project
def test_each_model_gets_its_own_merged_results_graph():
    project = _project()
    graph_a = project.join_graph.merged_results_graph(project.get_model("model_a"))
    graph_b = project.join_graph.merged_results_graph(project.get_model("model_b"))

    assert "a_orders.total" in graph_a and "b_events.total" not in graph_a
    assert "b_events.total" in graph_b and "a_orders.total" not in graph_b
    assert project.join_graph.merged_results_graph(project.get_model("model_a")) is graph_a
