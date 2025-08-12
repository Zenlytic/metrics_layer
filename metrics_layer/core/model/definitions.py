from metrics_layer.core.exceptions import MetricsLayerException


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


def sql_flavor_to_sqlglot_format(zenlytic_sql_flavor: str) -> str:
    sql_flavor = zenlytic_sql_flavor.upper()
    if sql_flavor == Definitions.snowflake:
        return Definitions.snowflake.lower()
    elif sql_flavor == Definitions.bigquery:
        return Definitions.bigquery.lower()
    elif sql_flavor == Definitions.redshift:
        return Definitions.redshift.lower()
    elif sql_flavor == Definitions.postgres:
        return Definitions.postgres.lower()
    elif sql_flavor == Definitions.druid:
        return Definitions.druid.lower()
    elif sql_flavor == Definitions.sql_server:
        return "tsql"
    elif sql_flavor == Definitions.duck_db:
        return Definitions.duck_db.lower().replace("_", "")
    elif sql_flavor == Definitions.databricks:
        return Definitions.databricks.lower()
    elif sql_flavor == Definitions.azure_synapse:
        return "tsql"
    elif sql_flavor == Definitions.trino:
        return Definitions.trino.lower()
    elif sql_flavor == Definitions.mysql:
        return Definitions.mysql.lower()
    else:
        raise MetricsLayerException(f"Unknown SQL flavor: {zenlytic_sql_flavor}")
