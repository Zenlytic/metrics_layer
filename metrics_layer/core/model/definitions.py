class Definitions:
    snowflake = "SNOWFLAKE"
    bigquery = "BIGQUERY"
    redshift = "REDSHIFT"
    postgres = "POSTGRES"
    supported_warehouses = [snowflake, bigquery, redshift, postgres]
    supported_warehouses_text = ", ".join(supported_warehouses)

    does_not_exist = "__DOES_NOT_EXIST__"
