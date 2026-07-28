import copy
import gc
import pickle

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.project import Project


def _make_project(models, views, dashboards, topics, manifest):
    # deepcopy views: Project.__init__ mutates them via _handle_join_as_duplication
    return Project(
        models=models,
        views=copy.deepcopy(views),
        dashboards=dashboards,
        topics=topics,
        looker_env="prod",
        connection_lookup={"connection_name": "SNOWFLAKE"},
        manifest=manifest,
    )


def _count_live(type_name):
    gc.collect()
    return len([o for o in gc.get_objects() if type(o).__name__ == type_name])


def test_get_field_is_memoized_within_instance(project):
    first = project.get_field("orders.number_of_orders")
    second = project.get_field("orders.number_of_orders")
    assert first is second
    assert "get_field" in project._instance_memo


def test_refresh_cache_clears_instance_memo(fresh_project):
    fresh_project.get_field("orders.number_of_orders")
    fresh_project.fields()
    assert fresh_project._instance_memo  # populated
    fresh_project.refresh_cache()
    assert fresh_project._instance_memo == {}
    assert fresh_project._join_graph is None


def test_projects_released_after_field_access(models, views, dashboards, topics, manifest):
    baseline = _count_live("Project")
    for _ in range(20):
        p = _make_project(models, views, dashboards, topics, manifest)
        p.get_field("orders.number_of_orders")
        p.fields()
        del p
    assert _count_live("Project") == baseline


def test_field_join_graphs_memoized_within_instance(project):
    field = project.get_field("orders.number_of_orders")
    assert field.join_graphs() is field.join_graphs()
    assert "join_graphs" in field._instance_memo


def test_field_referenced_fields_memoized_within_instance(project):
    field = project.get_field("orders.number_of_orders")
    a = field.referenced_fields("${TABLE}.id")
    b = field.referenced_fields("${TABLE}.id")
    assert a is b
    assert "referenced_fields" in field._instance_memo


_FUNNEL = {
    "steps": [
        [{"field": "channel", "expression": "equal_to", "value": "Paid"}],
        [{"field": "channel", "expression": "isin", "value": ["Organic", "Email"]}],
    ],
    "within": {"value": 3, "unit": "days"},
}


def _make_connection(models, views, dashboards, topics, manifest, connections):
    project = _make_project(models, views, dashboards, topics, manifest)
    return MetricsLayerConnection(project=project, connections=connections)


def test_funnel_query_objects_released(
    models, views, dashboards, topics, manifest, connections
):
    baseline = _count_live("FunnelQuery")
    for _ in range(10):
        conn = _make_connection(models, views, dashboards, topics, manifest, connections)
        conn.get_sql_query(metrics=["number_of_orders"], funnel=_FUNNEL)
        del conn
    assert _count_live("FunnelQuery") == baseline


def test_cumulative_query_objects_released(
    models, views, dashboards, topics, manifest, connections
):
    baseline = _count_live("CumulativeMetricsQuery")
    for _ in range(10):
        conn = _make_connection(models, views, dashboards, topics, manifest, connections)
        conn.get_sql_query(metrics=["total_lifetime_revenue"])
        del conn
    assert _count_live("CumulativeMetricsQuery") == baseline


def test_pickle_round_trip_with_populated_memo(fresh_project):
    # Cached Field objects must not be serialized: MetricsLayerBase.__getattr__
    # makes them un-unpicklable (infinite recursion on the missing _definition),
    # and the memo is derived state that each instance rebuilds on demand.
    fresh_project.get_field("orders.number_of_orders")
    assert fresh_project._instance_memo

    restored = pickle.loads(pickle.dumps(fresh_project))
    assert restored._instance_memo == {}
    assert restored.get_field("orders.number_of_orders") is not None


def test_shallow_copied_project_has_independent_field_cache(
    models, views, dashboards, topics, manifest
):
    from copy import copy

    original = _make_project(models, views, dashboards, topics, manifest)
    original.get_field("orders.number_of_orders")  # populate the original's memo

    duplicate = copy(original)  # mirrors query_generator.py's non-additive flow
    assert duplicate._instance_memo is not original._instance_memo

    # A temp field added only to the copy (shared _views, refresh_cache=False) must be
    # visible to the copy — the copy computes lookups against its own view state.
    duplicate.add_field(
        {
            "name": "temp_min_measure",
            "field_type": "measure",
            "type": "min",
            "sql": "${orders.order_raw}",
        },
        view_name="orders",
        refresh_cache=False,
    )
    assert duplicate.get_field("orders.temp_min_measure") is not None
