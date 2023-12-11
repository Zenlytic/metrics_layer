import pytest

from metrics_layer.cli.seeding import SeedMetricsLayer


@pytest.mark.seeding
@pytest.mark.parametrize("db_conn", ["testing_snowflake", "testing_bigquery"])
def test_seeding_table_query(connection, db_conn):
    seeder = SeedMetricsLayer(database="test", metrics_layer=connection, connection=db_conn)
    table_query = seeder.table_query()

    if db_conn == "testing_snowflake":
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_owner as table_owner, table_type as table_type, "
            "bytes as table_size, created as table_created, last_altered as table_last_modified, "
            "row_count as table_row_count, comment as comment FROM test.INFORMATION_SCHEMA.TABLES;"
        )
    else:
        correct = (
            "SELECT table_catalog as table_database, table_schema as table_schema, "
            "table_name as table_name, table_type as table_type, "
            "creation_time as table_created FROM `test.test_schema`.INFORMATION_SCHEMA.TABLES;"
        )

    assert table_query == correct
