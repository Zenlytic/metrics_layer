import pytest

from metrics_layer.core.model.definitions import Definitions


#  cumulative metric only
#  cumulative metric with another dim
#  cumualtive metric with another metric
#  cumulative metric with another metric and dim


@pytest.mark.skip("Hold off on cumulative types until initial migration is done")
@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_cumulative_query_metric_only(connection, query_type):
    query = connection.get_sql_query(
        metrics=["total_lifetime_revenue", "cumulative_customers"], query_type=query_type
    )

    correct = ""
    query == correct
