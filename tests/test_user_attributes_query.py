import pytest

# from metrics_layer.core.sql.query_errors import ArgumentError


@pytest.mark.queryy
def test_query_user_attribute_in_sql(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["product_name_lang", "product_name"],
    )

    correct = ""
    assert query == correct
