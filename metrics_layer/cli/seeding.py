import os

from metrics_layer.core.model.definitions import Definitions


class SeedMetricsLayer:
    default_views_path = "views"
    default_models_path = "models"
    default_dashboards_path = "dashboards"

    def __init__(self, profile, connection=None, database=None, schema=None, table=None):
        self.default_model_name = "base_model"
        self.profile_name = profile
        self.metrics_layer, self.connection = self._init_connection(self.profile_name, connection)
        self.database = database if database else self.connection.database
        self.schema = schema if schema else self.connection.schema
        self.table = table
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
            "TEXT": "string",
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

    def seed(self):
        from metrics_layer.core.parse import ProjectDumper, ProjectLoader

        if self.connection.type not in {Definitions.snowflake, Definitions.bigquery, Definitions.redshift}:
            raise NotImplementedError(
                "The only data warehouses supported for seeding are Snowflake, Redshift and BigQuery"
            )
        table_query = self.table_query()
        data = self.run_query(table_query)
        data.columns = [c.upper() for c in data.columns]
        print(f"Got information on all tables and views in {self.location_description}")

        # We need to filter down the whole result to just the chosen schema and table (if applicable)
        if self.schema:
            data = data[data["TABLE_SCHEMA"].str.lower() == self.schema.lower()].copy()

        if self.table:
            data = data[data["TABLE_NAME"].str.lower() == self.table.lower()].copy()

        folder = self._location()
        loader = ProjectLoader(folder)
        project = loader.load()

        current_models = project.models()

        model_name = self.get_model_name(current_models)
        # Each iteration represents either a single table or a single view
        views, tables = [], data["TABLE_NAME"].unique()
        for i, table_name in enumerate(tables):
            column_df = data[data["TABLE_NAME"].str.lower() == table_name.lower()].copy()
            schema_name = column_df["TABLE_SCHEMA"].values[0]
            view = self.make_view(column_df, model_name, table_name, schema_name)

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
        ProjectDumper(models, model_folder, views, view_folder).dump(folder)

    def get_model_name(self, current_models: list):
        if len(current_models) > 0:
            return current_models[0]["name"]
        return self.default_model_name

    def make_models(self):
        model = {
            "version": 1,
            "type": "model",
            "name": self.default_model_name,
            "connection": self.connection.name,
        }
        return [model]

    def make_view(self, column_data, model_name: str, table_name: str, schema_name: str):
        view_name = self.clean_name(table_name)
        count_measure = {"field_type": "measure", "name": "count", "type": "count"}
        fields = self.make_fields(column_data) + [count_measure]
        if self.connection.type in {Definitions.snowflake, Definitions.redshift}:
            sql_table_name = f"{schema_name}.{table_name}"
        elif self.connection.type == Definitions.bigquery:
            sql_table_name = f"`{self.database}.{schema_name}.{table_name}`"
        view = {
            "version": 1,
            "type": "view",
            "name": view_name,
            "model_name": model_name,
            "sql_table_name": sql_table_name,
            "default_date": next((f["name"] for f in fields if f["field_type"] == "dimension_group"), None),
            "row_label": "TODO - Label row",
            "fields": fields,
        }
        return view

    def make_fields(self, column_data: str):
        fields = []
        for _, row in column_data[["COLUMN_NAME", "DATA_TYPE"]].iterrows():
            name = self.clean_name(row["COLUMN_NAME"])
            if self.connection.type == Definitions.snowflake:
                metrics_layer_type = self._snowflake_type_lookup.get(row["DATA_TYPE"], "string")
            if self.connection.type == Definitions.redshift:
                metrics_layer_type = self._redshift_type_lookup.get(row["DATA_TYPE"], "string")
            elif self.connection.type == Definitions.bigquery:
                metrics_layer_type = self._bigquery_type_lookup.get(row["DATA_TYPE"], "string")
            sql = "${TABLE}." + row["COLUMN_NAME"]

            field = {"name": name, "sql": sql}

            if metrics_layer_type in {"timestamp", "date"}:
                field["field_type"] = "dimension_group"
                field["type"] = "time"
                field["timeframes"] = ["raw", "date", "week", "month", "quarter", "year"]
                field["datatype"] = metrics_layer_type
            else:
                field["field_type"] = "dimension"
                field["type"] = metrics_layer_type
            fields.append(field)
        return fields

    def table_query(self):
        query = "SELECT table_catalog, table_schema, table_name, column_name, data_type FROM "
        if self.database and self.connection.type in {Definitions.snowflake, Definitions.redshift}:
            query += f"{self.database}.INFORMATION_SCHEMA.COLUMNS"
        elif self.database and self.schema and self.connection.type == Definitions.bigquery:
            query += f"`{self.database}.{self.schema}`.INFORMATION_SCHEMA.COLUMNS"
        elif not self.schema and self.connection.type == Definitions.bigquery:
            raise ValueError(
                "You must specify a database (project) AND a schema (dataset) for seeding in BigQuery"
            )
        else:
            raise ValueError("You must specify at least a database for seeding")
        return query + ";"

    def run_query(self, query: str):
        return self.metrics_layer.run_query(
            query, self.connection, run_pre_queries=False, start_warehouse=True
        )

    @staticmethod
    def _init_connection(profile_name: str, connection_name: str = None):
        metrics_layer = SeedMetricsLayer._init_profile(profile_name)

        if connection_name:
            connection = metrics_layer.get_connection(connection_name)
        else:
            connections = metrics_layer.list_connections()
            if len(connections) == 1:
                connection = connections[0]
            else:
                raise ValueError(
                    f"Could not determine the connection to use with profile {profile_name}, "
                    "please pass the connection name with the --connection arg"
                )
        return metrics_layer, connection

    @staticmethod
    def _init_profile(profile_name: str):
        from metrics_layer.core import MetricsLayerConnection

        connections = MetricsLayerConnection.get_connections_from_profile(profile_name)
        metrics_layer = MetricsLayerConnection(location=SeedMetricsLayer._location(), connections=connections)
        return metrics_layer

    @staticmethod
    def get_profile():
        from core.parse.project_reader_base import ProjectReaderBase

        zenlytic_project_path = os.path.join(SeedMetricsLayer._location(), "zenlytic_project.yml")
        dbt_project_path = os.path.join(SeedMetricsLayer._location(), "dbt_project.yml")

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
    def _init_directories():
        models_dir = SeedMetricsLayer.default_models_path
        views_dir = SeedMetricsLayer.default_views_path
        dashboards_dir = SeedMetricsLayer.default_dashboards_path

        # Create models and views directory inside of project dir
        for directory in [models_dir, views_dir, dashboards_dir]:
            fully_qualified_path = os.path.join(SeedMetricsLayer._location(), directory)
            if not os.path.exists(fully_qualified_path):
                os.mkdir(fully_qualified_path)

    @staticmethod
    def _init_project_file():
        from metrics_layer.core.parse import ProjectDumper

        models_dir = SeedMetricsLayer.default_models_path
        views_dir = SeedMetricsLayer.default_views_path
        dashboards_dir = SeedMetricsLayer.default_dashboards_path
        default_profile = {
            "name": "zenlytic_project_name",
            "profile": "my_dbt_profile",
            "model-paths": [models_dir],
            "view-paths": [views_dir],
            "dashboard-paths": [dashboards_dir],
        }
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
        return txt.lower().replace(" ", "_")
