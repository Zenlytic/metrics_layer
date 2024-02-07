import os
import re
import pandas as pd
from metrics_layer.core.model.definitions import Definitions
from typing import List


class SeedMetricsLayer:
    default_views_path = "views"
    default_models_path = "models"
    default_dashboards_path = "dashboards"

    def __init__(
        self,
        connection=None,
        database=None,
        schema=None,
        table=None,
        profile=None,
        target=None,
        metrics_layer=None,
        run_query_override=None,
    ):
        self.default_model_name = "base_model"
        self.run_query_override = run_query_override
        self.profile_name = profile
        self.target_name = target
        if metrics_layer is None:
            self.metrics_layer = SeedMetricsLayer._init_profile(self.profile_name, self.target_name)
        else:
            self.metrics_layer = metrics_layer
        self.connection = self._init_connection(self.metrics_layer, connection)
        self.database = database if database else self.connection.database
        self._database_is_not_default = database and database != self.connection.database
        self.schema = schema if schema else self.connection.schema
        self.table = table.replace(".sql", "") if table else None
        if schema and table:
            self.location_description = f"table: {self.database}.{self.schema}.{self.table}"
        elif schema:
            self.location_description = f"schema: {self.database}.{self.schema}"
        else:
            self.location_description = f"database: {self.database}"

        self._snowflake_type_lookup = {
            "DATE": "date",
            "DATETIME": "timestamp",
            "TIMESTAMP_TZ": "timestamp",
            "TIMESTAMP_NTZ": "timestamp",
            "TIMESTAMP_LTZ": "timestamp",
            "TIMESTAMP": "timestamp",
            "BOOLEAN": "yesno",
            "FIXED": "number",
            "FLOAT": "number",
            "REAL": "number",
            "TEXT": "string",
            "VARCHAR": "string",
            "CHAR": "string",
            "CHARACTER": "string",
            "STRING": "string",
            "BINARY": "string",
            "VARBINARY": "string",
        }

        self._redshift_type_lookup = {
            "TIME": "timestamp",
            "TIMETZ": "timestamp",
            "TIMESTAMP": "timestamp",
            "TIMESTAMPTZ": "timestamp",
            "DATE": "date",
            "CHAR": "string",
            "VARCHAR": "string",
            "BOOLEAN": "yesno",
            "DOUBLE PRECISION": "number",
            "DOUBLE": "number",
            "PRECISION": "number",
            "REAL": "number",
            "DECIMAL": "number",
            "BIGINT": "number",
        }
        self._databricks_type_lookup = {
            "TIME": "timestamp",
            "TIMESTAMP": "timestamp",
            "TIMESTAMP_NTZ": "timestamp",
            "DATE": "date",
            "STRING": "string",
            "BOOLEAN": "yesno",
            "DOUBLE": "number",
            "FLOAT": "number",
            "DECIMAL": "number",
            "LONG": "number",
            "INT": "number",
            "SMALLINT": "number",
            "BIGINT": "number",
            "TINYINT": "number",
        }
        self._druid_type_lookup = {
            "CHAR": "string",
            "VARCHAR": "string",
            "DECIMAL": "number",
            "FLOAT": "number",
            "REAL": "number",
            "DOUBLE": "number",
            "BOOLEAN": "yesno",
            "TINYINT": "number",
            "SMALLINT": "number",
            "INTEGER": "number",
            "BIGINT": "number",
            "TIMESTAMP": "timestamp",
            "DATE": "date",
        }
        # Also duck db type lookup
        self._postgres_type_lookup = {
            "date": "date",
            "timestamp without time zone": "timestamp",
            "timestamp with time zone": "timestamp",
            "text": "string",
            "boolean": "yesno",
            "bigint": "number",
            "integer": "number",
            "smallint": "number",
            "real": "number",
            "double precision": "number",
            "numeric": "number",
            "float": "number",
            "money": "number",
        }
        self._sql_server_type_lookup = {
            "float": "number",
            "real": "number",
            "bigint": "number",
            "numeric": "number",
            "smallint": "number",
            "decimal": "number",
            "smallmoney": "number",
            "int": "number",
            "tinyint": "number",
            "money": "number",
            "char": "string",
            "varchar": "string",
            "text": "string",
            "date": "date",
            "datetime2": "datetime",
            "smalldatetime": "datetime",
            "datetime": "datetime",
            "time": "datetime",
            "bit": "yesno",
        }
        self._bigquery_type_lookup = {
            "DATE": "date",
            "DATETIME": "timestamp",
            "TIMESTAMP": "timestamp",
            "BOOL": "yesno",
            "FLOAT64": "number",
            "INT64": "number",
            "NUMERIC": "number",
            "STRING": "string",
        }

    def seed(self, auto_tag_searchable_fields: bool = False):
        from metrics_layer.core.parse import ProjectDumper, ProjectLoader

        if self.connection.type not in Definitions.supported_warehouses:
            raise NotImplementedError(
                f"The only data warehouses supported for seeding are {Definitions.supported_warehouses_text}"
            )
        columns_query = self.columns_query()
        data = self.run_query(columns_query)
        data.columns = [c.upper() for c in data.columns]

        if self.connection.type in {Definitions.snowflake, Definitions.databricks}:
            table_query = self.table_query()
            table_data = self.run_query(table_query)
            table_data.columns = [c.upper() for c in table_data.columns]
        else:
            table_data = pd.DataFrame()
        print(f"Got information on all tables and views in {self.location_description}")

        # We need to filter down the whole result to just the chosen schema and table (if applicable)
        if self.schema:
            data = data[data["TABLE_SCHEMA"].str.lower() == self.schema.lower()].copy()
            if not table_data.empty:
                table_data = table_data[table_data["TABLE_SCHEMA"].str.lower() == self.schema.lower()].copy()

        if self.table:
            data = data[data["TABLE_NAME"].str.lower() == self.table.lower()].copy()
            if not table_data.empty:
                table_data = table_data[table_data["TABLE_NAME"].str.lower() == self.table.lower()].copy()
        folder = self._location()
        loader = ProjectLoader(folder)
        project = loader.load()

        current_models = project.models()

        model_name = self.get_model_name(current_models)
        # Each iteration represents either a single table or a single view
        views, tables = [], data["TABLE_NAME"].unique()
        for i, table_name in enumerate(tables):
            column_df = data[data["TABLE_NAME"].str.lower() == table_name.lower()].copy()
            if not table_data.empty:
                table_comment = table_data[table_data["TABLE_NAME"].str.lower() == table_name.lower()][
                    "COMMENT"
                ].values[0]
            else:
                table_comment = None
            schema_name = column_df["TABLE_SCHEMA"].values[0]
            view = self.make_view(
                column_df,
                model_name,
                table_name,
                schema_name,
                table_comment,
                auto_tag_searchable_fields=auto_tag_searchable_fields,
            )
            if self.table:
                progress = ""
            else:
                progress = f"({i + 1} / {len(tables)})"
            print(f"Got information on {table_name} {progress}")
            views.append(view)

        if len(current_models) > 0:
            models = []
        else:
            models = self.make_models()

        model_folder = loader.zenlytic_project.get("model-paths", [self.default_models_path])[0]
        view_folder = loader.zenlytic_project.get("view-paths", [self.default_views_path])[0]

        # Dump the models to yaml files
        dumper = ProjectDumper(models, model_folder, views, view_folder)
        dumper.dump(folder)

        # Add the zenlytic_project.yml file if it doesn't exist yet
        if len(current_models) == 0 and not os.path.exists(os.path.join(folder, "zenlytic_project.yml")):
            zenlytic_project_path = os.path.join(folder, "zenlytic_project.yml")
            project_data = {
                "name": self.connection.name,
                "profile": self.connection.name,
                "model-paths": ["models"],
                "view-paths": ["views"],
                "dashboard-paths": ["dashboards"],
            }
            dumper.dump_yaml_file(project_data, zenlytic_project_path)

    def get_model_name(self, current_models: list):
        if len(current_models) > 0:
            return current_models[0].name
        return self.default_model_name

    def make_models(self):
        model = {
            "version": 1,
            "type": "model",
            "name": self.default_model_name,
            "connection": self.connection.name,
        }
        return [model]

    def make_view(
        self,
        column_data,
        model_name: str,
        table_name: str,
        schema_name: str,
        table_comment: str = None,
        auto_tag_searchable_fields: bool = True,
    ):
        view_name = self.clean_name(table_name)
        fields = self.make_fields(
            column_data,
            schema_name=schema_name,
            table_name=table_name,
            auto_tag_searchable_fields=auto_tag_searchable_fields,
        )
        if self.connection.type in {
            Definitions.snowflake,
            Definitions.redshift,
            Definitions.postgres,
            Definitions.sql_server,
            Definitions.azure_synapse,
            Definitions.duck_db,
            Definitions.databricks,
        }:
            sql_table_name = f"{schema_name}.{table_name}"
            if self._database_is_not_default:
                sql_table_name = f"{self.database}.{sql_table_name}"
        elif self.connection.type == Definitions.druid:
            sql_table_name = f"{schema_name}.{table_name}"
        elif self.connection.type == Definitions.bigquery:
            sql_table_name = f"`{self.database}.{schema_name}.{table_name}`"
        else:
            raise NotImplementedError(f"Unsupported connection type {self.connection.type}")
        view = {
            "version": 1,
            "type": "view",
            "name": view_name,
            "model_name": model_name,
            "sql_table_name": sql_table_name,
            "default_date": next((f["name"] for f in fields if f["field_type"] == "dimension_group"), None),
            "fields": fields,
        }
        if table_comment:
            view["description"] = table_comment
        if view["default_date"] is None:
            view.pop("default_date")
        return view

    def make_fields(self, column_data, schema_name: str, table_name: str, auto_tag_searchable_fields: bool):
        if auto_tag_searchable_fields:
            if schema_name is None:
                raise ValueError("schema_name is required to auto tag searchable fields")
            if table_name is None:
                raise ValueError("table_name is required to auto tag searchable fields")

        fields = []
        searchable_field_candidates = []
        if self.connection.type in {Definitions.snowflake, Definitions.databricks}:
            data_to_iterate = column_data[["COLUMN_NAME", "DATA_TYPE", "COMMENT"]]
        else:
            data_to_iterate = column_data[["COLUMN_NAME", "DATA_TYPE"]]
        for _, row in data_to_iterate.iterrows():
            name = self.clean_name(row["COLUMN_NAME"])
            if self.connection.type == Definitions.snowflake:
                metrics_layer_type = self._snowflake_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type == Definitions.redshift:
                metrics_layer_type = self._redshift_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type in {Definitions.postgres, Definitions.duck_db}:
                metrics_layer_type = self._postgres_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type == Definitions.bigquery:
                metrics_layer_type = self._bigquery_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type == Definitions.druid:
                metrics_layer_type = self._druid_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type in {Definitions.sql_server, Definitions.azure_synapse}:
                metrics_layer_type = self._sql_server_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type == Definitions.databricks:
                metrics_layer_type = self._databricks_type_lookup.get(row["DATA_TYPE"], "string")
            else:
                raise NotImplementedError(f"Unknown connection type: {self.connection.type}")
            sql = "${TABLE}." + row["COLUMN_NAME"]

            field = {"name": name, "sql": sql}

            if "COMMENT" in row and row["COMMENT"] is not None:
                field["description"] = row["COMMENT"]

            if metrics_layer_type in {"timestamp", "date", "datetime"}:
                field["field_type"] = "dimension_group"
                field["type"] = "time"
                field["timeframes"] = [
                    "raw",
                    "date",
                    "day_of_year",
                    "week",
                    "week_of_year",
                    "month",
                    "month_of_year",
                    "quarter",
                    "year",
                ]
                field["datatype"] = metrics_layer_type
            elif metrics_layer_type == "string" and auto_tag_searchable_fields:
                field["field_type"] = "dimension"
                field["type"] = "string"
                searchable_field_candidates.append(row["COLUMN_NAME"])
            else:
                field["field_type"] = "dimension"
                field["type"] = metrics_layer_type
            fields.append(field)
        if searchable_field_candidates:
            column_cardinalities_query = self.column_cardinalities_query(
                column_names=searchable_field_candidates, schema_name=schema_name, table_name=table_name
            )
            column_cardinalities = self.run_query(query=column_cardinalities_query)

            # Get the column names that have a cardinality of less than 100
            # Note: running the query doesn't preserve the column name cases
            searchable_column_names = [
                col.lower().rsplit("_cardinality", 1)[0]
                for col in column_cardinalities.columns
                if column_cardinalities.loc[0, col] < 100
            ]

            for field in fields:
                if field["sql"].split(".", 1)[1].lower() in searchable_column_names:
                    field["searchable"] = True

        return fields

    def column_cardinalities_query(self, column_names: List[str], schema_name: str, table_name: str) -> str:
        cardinality_queries = []
        for column_name in column_names:
            if self.connection.type in (Definitions.snowflake, Definitions.duck_db, Definitions.druid):
                query = (
                    f'APPROX_COUNT_DISTINCT( "{column_name}" ) as "{column_name}_cardinality"'  # noqa: E501
                )
            elif self.connection.type == Definitions.redshift:
                query = f'APPROXIMATE COUNT(DISTINCT "{column_name}" ) as "{column_name}_cardinality"'  # noqa: E501
            elif self.connection.type == Definitions.postgres:
                query = f'COUNT(DISTINCT "{column_name}" ) as "{column_name}_cardinality"'  # noqa: E501
            elif self.connection.type in {Definitions.sql_server, Definitions.azure_synapse}:
                query = (
                    f'APPROX_COUNT_DISTINCT( "{column_name}" ) as "{column_name}_cardinality"'  # noqa: E501
                )
            elif self.connection.type in {Definitions.bigquery, Definitions.databricks}:
                query = f"APPROX_COUNT_DISTINCT( `{column_name}` ) as `{column_name}_cardinality`"
            else:
                raise NotImplementedError(f"Unknown connection type: {self.connection.type}")
            cardinality_queries.append(query)

        query = f"SELECT {', '.join(cardinality_queries)}"

        if self.connection.type in {
            Definitions.snowflake,
            Definitions.duck_db,
            Definitions.druid,
            Definitions.redshift,
            Definitions.postgres,
            Definitions.sql_server,
            Definitions.azure_synapse,
            Definitions.databricks,
        }:
            query += f" FROM {self.database}.{schema_name}.{table_name}"
        elif self.connection.type == Definitions.bigquery:
            query += f" FROM `{self.database}`.`{schema_name}`.`{table_name}`"

        return query + ";" if self.connection.type != Definitions.druid else query

    def columns_query(self):
        if self.connection.type in {Definitions.snowflake, Definitions.databricks}:
            comment_statement = ", comment as comment"
        else:
            comment_statement = ""
        query = (
            f"SELECT table_catalog, table_schema, table_name, column_name, data_type{comment_statement} FROM "
        )
        if self.database and self.connection.type in {
            Definitions.snowflake,
            Definitions.redshift,
            Definitions.postgres,
            Definitions.sql_server,
            Definitions.azure_synapse,
            Definitions.duck_db,
            Definitions.databricks,
        }:
            if self.connection.type == Definitions.databricks and self.database is None:
                query += f"INFORMATION_SCHEMA.COLUMNS"
            else:
                query += f"{self.database}.INFORMATION_SCHEMA.COLUMNS"
        elif self.connection.type == Definitions.druid:
            query = (
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS"
            )
        elif self.database and self.schema and self.connection.type == Definitions.bigquery:
            query += f"`{self.database}.{self.schema}`.INFORMATION_SCHEMA.COLUMNS"
        elif not self.schema and self.connection.type == Definitions.bigquery:
            raise ValueError(
                "You must specify a database (project) AND a schema (dataset) for seeding in BigQuery"
            )
        else:
            raise ValueError("You must specify at least a database for seeding")
        return query + ";" if self.connection.type != Definitions.druid else query

    def table_query(self):
        if self.database and self.connection.type == Definitions.snowflake:
            query = (
                "SELECT table_catalog as table_database, table_schema as table_schema, "
                "table_name as table_name, table_owner as table_owner, table_type as table_type, "
                "bytes as table_size, created as table_created, last_altered as table_last_modified, "
                "row_count as table_row_count, comment as comment "
                f"FROM {self.database}.INFORMATION_SCHEMA.TABLES"
            )
        elif self.connection.type in {Definitions.druid}:
            query = (
                "SELECT TABLE_CATALOG as table_database, TABLE_SCHEMA as table_schema, "
                "TABLE_NAME as table_name, TABLE_TYPE as table_type "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA not in ('sys', 'INFORMATION_SCHEMA')"
            )
        elif self.database and self.connection.type in {
            Definitions.redshift,
            Definitions.postgres,
            Definitions.sql_server,
            Definitions.azure_synapse,
            Definitions.duck_db,
        }:
            query = (
                "SELECT table_catalog as table_database, table_schema as table_schema, "
                "table_name as table_name, table_type as table_type "
                f"FROM {self.database}.INFORMATION_SCHEMA.TABLES "
                "WHERE table_schema not in ('pg_catalog', 'information_schema')"
            )
        elif self.connection.type == Definitions.databricks:
            database_str = f"{self.database}." if self.database else ""
            query = (
                "SELECT table_catalog as table_database, table_schema as table_schema, "
                "table_name as table_name, table_type as table_type, "
                f"comment as comment FROM {database_str}INFORMATION_SCHEMA.TABLES"
            )
        elif self.database and self.schema and self.connection.type == Definitions.bigquery:
            query = (
                "SELECT table_catalog as table_database, table_schema as table_schema, "
                "table_name as table_name, table_type as table_type, "
                "creation_time as table_created FROM "
                f"`{self.database}.{self.schema}`.INFORMATION_SCHEMA.TABLES"
            )
        elif not self.schema and self.connection.type == Definitions.bigquery:
            raise ValueError(
                "You must specify a database (project) AND a schema (dataset) for seeding in BigQuery"
            )
        else:
            raise ValueError("You must specify at least a database for seeding")
        return query + ";" if self.connection.type != Definitions.druid else query

    def run_query(self, query: str):
        if self.run_query_override:
            return self.run_query_override(query)
        return self.metrics_layer.run_query(
            query, self.connection, run_pre_queries=False, start_warehouse=True
        )

    @staticmethod
    def _init_connection(metrics_layer, connection_name: str = None):
        if connection_name:
            connection = metrics_layer.get_connection(connection_name)
        else:
            connections = metrics_layer.list_connections()
            if len(connections) == 1:
                connection = connections[0]
            else:
                raise ValueError(
                    f"Could not determine the connection to use, "
                    "please pass the connection name with the --connection arg"
                )
        return connection

    @staticmethod
    def _init_profile(profile_name: str, target: str = None):
        from metrics_layer.core import MetricsLayerConnection

        connections = MetricsLayerConnection.get_connections_from_profile(profile_name, target)
        metrics_layer = MetricsLayerConnection(location=SeedMetricsLayer._location(), connections=connections)
        metrics_layer.load()
        return metrics_layer

    @staticmethod
    def get_profile():
        from metrics_layer.core.parse.project_reader_base import ProjectReaderBase

        zenlytic_project_path = os.path.join(SeedMetricsLayer._location(), "zenlytic_project.yml")
        dbt_project_path = SeedMetricsLayer._dbt_project_path()

        if os.path.exists(zenlytic_project_path):
            zenlytic_project = ProjectReaderBase.read_yaml_file(zenlytic_project_path)
            return zenlytic_project["profile"]
        elif os.path.exists(dbt_project_path):
            dbt_project = ProjectReaderBase.read_yaml_file(dbt_project_path)
            return dbt_project["profile"]
        raise ValueError(
            """Could not find a profile for the metrics layer in either the zenlytic_project.yml file or in
         the dbt_project.yml file, if neither of those files exist, please create the file"""
        )

    @staticmethod
    def _dbt_project_path():
        return os.path.join(SeedMetricsLayer._location(), "./dbt_project.yml")

    @staticmethod
    def _in_dbt_project():
        return os.path.exists(SeedMetricsLayer._dbt_project_path())

    def _dbt_project_file():
        from metrics_layer.core.parse.project_reader_base import ProjectReaderBase

        if SeedMetricsLayer._in_dbt_project():
            return ProjectReaderBase.read_yaml_file(SeedMetricsLayer._dbt_project_path())
        return None

    @staticmethod
    def _init_directories():
        models_dir = SeedMetricsLayer.default_models_path
        views_dir = SeedMetricsLayer.default_views_path
        dashboards_dir = SeedMetricsLayer.default_dashboards_path

        in_dbt_project = SeedMetricsLayer._in_dbt_project()
        if in_dbt_project:
            to_create = [dashboards_dir]
        else:
            to_create = [models_dir, views_dir, dashboards_dir]

        # Create models and views directory inside of project dir
        for directory in to_create:
            fully_qualified_path = os.path.join(SeedMetricsLayer._location(), directory)
            if not os.path.exists(fully_qualified_path):
                os.mkdir(fully_qualified_path)

    @staticmethod
    def _init_project_file():
        from metrics_layer.core.parse import ProjectDumper

        common = {"dashboard-paths": [SeedMetricsLayer.default_dashboards_path]}
        if SeedMetricsLayer._in_dbt_project():
            dbt_project = SeedMetricsLayer._dbt_project_file()
            default_profile = {"name": dbt_project["name"], "profile": dbt_project["profile"], "mode": "dbt"}
        else:
            default_profile = {
                "name": "zenlytic_project_name",
                "profile": "my_dbt_profile",
                "model-paths": [SeedMetricsLayer.default_models_path],
                "view-paths": [SeedMetricsLayer.default_views_path],
            }

        default_profile = {**default_profile, **common}
        ProjectDumper.dump_yaml_file(default_profile, "zenlytic_project.yml")

    @staticmethod
    def _location():
        return os.getcwd()

    @staticmethod
    def _test_git():
        try:
            import git  # noqa

            valid = True
        except ImportError:
            valid = False
        return valid

    @staticmethod
    def clean_name(txt: str):
        alphanumeric_string = re.sub(r"[^a-zA-Z0-9\s_]", "", txt)
        return alphanumeric_string.lower().strip().replace(" ", "_")
