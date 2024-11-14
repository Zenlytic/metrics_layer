import pytest

from metrics_layer.core.model.project import Project
from metrics_layer.core.sql.resolve import SQLQueryResolver


@pytest.mark.query
def test_empty_query(models, views, connections):
    SQLQueryResolver(
        metrics=[],
        dimensions=[],
        project=Project(models=models, views=views),
        connections=connections,
    )
