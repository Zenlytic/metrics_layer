import pytest

from metrics_layer.cli.seeding import SeedMetricsLayer


@pytest.mark.seeding
@pytest.mark.parametrize(
    "db_name,db_conn",
    [
        ("test", "testing_snowflake"),
        ("test", "testing_bigquery"),
        ("test", "testing_databricks"),
        (None, "testing_databricks"),
    ],
)
def test_seeding_table_query(connection, db_name, db_conn):
    seeder = SeedMetricsLayer(database=db_name, metrics_layer=connection, connection=db_conn)
    table_query = seeder.table_query()

    if db_conn == "testing_snowflake":
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_owner as table_owner, table_type as table_type, "
            "bytes as table_size, created as table_created, last_altered as table_last_modified, "
            "row_count as table_row_count, comment as comment FROM test.INFORMATION_SCHEMA.TABLES;"
        )
    elif db_conn == "testing_bigquery":
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_type as table_type, "
            "creation_time as table_created FROM `test.test_schema`.INFORMATION_SCHEMA.TABLES;"
        )
    elif db_conn == "testing_databricks" and db_name == "test":
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_type as table_type, comment as comment"
            " FROM test.INFORMATION_SCHEMA.TABLES;"
        )
    elif db_conn == "testing_databricks" and db_name is None:
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_type as table_type, comment as comment"
            " FROM INFORMATION_SCHEMA.TABLES;"
        )
    else:
        raise ValueError(f"Unknown connection: {db_conn}")

    assert table_query == correct
