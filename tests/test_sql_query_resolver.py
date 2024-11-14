from metrics_layer.core.model.project import Project
from metrics_layer.core.sql.resolve import SQLQueryResolver


def test_empty_query(models, views, connections):
    SQLQueryResolver(
        metrics=[],
        dimensions=[],
        project=Project(models=models, views=views),
        connections=connections,
    )
