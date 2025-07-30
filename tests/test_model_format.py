import pytest

from metrics_layer.core.model.definitions import Definitions


@pytest.mark.query
@pytest.mark.parametrize("query_type", Definitions.supported_warehouses)
def test_measure_with_model_format_basic(connection, query_type):
    field = connection.project.get_field("order_lines.total_item_revenue")

    query = field.sql_query(query_type=query_type, model_format=True)

    assert query == "SUM(order_lines.revenue)"


@pytest.mark.query
def test_measure_with_model_format_with_complex_sql(connection):
    field = connection.project.get_field("order_lines.ending_on_hand_qty")

    query = field.sql_query(query_type="SNOWFLAKE", model_format=True)

    correct = (
        "CAST(SPLIT_PART(LISTAGG((order_lines.inventory_qty), ',') WITHIN GROUP (ORDER BY (DATE_TRUNC('DAY',"
        " order_lines.order_date)) DESC), ',', 0) AS INT)"
    )
    assert query == correct


@pytest.mark.query
def test_measure_with_model_format_with_window_function(connection):
    field = connection.project.get_field("order_lines.order_sequence")

    query = field.sql_query(query_type="SNOWFLAKE", model_format=True)

    correct = (
        "DENSE_RANK() OVER (PARTITION BY order_lines.customer_id ORDER BY DATE_TRUNC('DAY',"
        " order_lines.order_date) ASC)"
    )
    assert query == correct


@pytest.mark.query
def test_measure_with_model_format_with_non_additive_dimension(connection):
    field = connection.project.get_field("mrr.mrr_beginning_of_month")

    query = field.sql_query(query_type="SNOWFLAKE", model_format=True)

    correct = "mrr.mrr_beginning_of_month()"
    assert query == correct


@pytest.mark.query
def test_measure_with_model_format_with_non_additive_dimension_nested(connection):
    field = connection.project.get_field("mrr.mrr_change_per_billed_account")

    query = field.sql_query(query_type="SNOWFLAKE", model_format=True)

    correct = "((mrr.mrr_end_of_month()) - (mrr.mrr_beginning_of_month())) / (COUNT(mrr.parent_account_id))"
    assert query == correct


@pytest.mark.query
def test_measure_with_model_format_with_implicit_table_reference(connection):
    field = connection.project.get_field("other_db_traffic.other_traffic_campaign")

    query = field.sql_query(query_type="SNOWFLAKE", model_format=True)

    correct = 'other_db_traffic."traffic_campaign"'
    assert query == correct
