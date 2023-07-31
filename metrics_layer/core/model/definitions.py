class Definitions:
    snowflake = "SNOWFLAKE"
    bigquery = "BIGQUERY"
    redshift = "REDSHIFT"
    postgres = "POSTGRES"
    druid = "DRUID"
    supported_warehouses = [snowflake, bigquery, redshift, postgres, druid]
    supported_warehouses_text = ", ".join(supported_warehouses)

    does_not_exist = "__DOES_NOT_EXIST__"
    canon_date_join_graph_root = "canon_date_core"
