class Definitions:
    snowflake = "SNOWFLAKE"
    bigquery = "BIGQUERY"
    redshift = "REDSHIFT"
    postgres = "POSTGRES"
    druid = "DRUID"
    sql_server = "SQL_SERVER"
    duck_db = "DUCK_DB"
    databricks = "DATABRICKS"
    azure_synapse = "AZURE_SYNAPSE"
    trino = "TRINO"
    mysql = "MYSQL"
    supported_warehouses = [
        snowflake,
        bigquery,
        redshift,
        postgres,
        druid,
        sql_server,
        duck_db,
        databricks,
        azure_synapse,
        trino,
        mysql,
    ]
    symmetric_aggregates_supported_warehouses = [
        snowflake,
        redshift,
        bigquery,
        postgres,
        duck_db,
        azure_synapse,
        sql_server,
    ]
    no_semicolon_warehouses = [druid, trino]
    needs_datetime_cast = [bigquery, trino]
    supported_warehouses_text = ", ".join(supported_warehouses)

    does_not_exist = "__DOES_NOT_EXIST__"
    canon_date_join_graph_root = "canon_date_core"

    date_format_tz = "%Y-%m-%dT%H:%M:%SZ"
