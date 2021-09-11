import pytest

from granite.core.model.project import Project
from granite.core.sql.resolve import SQLResolverByQuery

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "fake",
    "explores": [{"name": "simple_explore", "from": "simple"}],
}

simple_view = {
    "type": "view",
    "name": "simple",
    "sql_table_name": "analytics.orders",
    "fields": [
        {"field_type": "measure", "type": "sum", "sql": "${TABLE}.revenue", "name": "total_revenue"},
        {"field_type": "dimension", "type": "string", "sql": "${TABLE}.sales_channel", "name": "channel"},
        {
            "field_type": "dimension",
            "type": "string",
            "sql": "${TABLE}.new_vs_repeat",
            "name": "new_vs_repeat",
        },
    ],
}


@pytest.mark.only
def test_simple_query():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(metrics=["total_revenue"], dimensions=["channel"], project=project)
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct
