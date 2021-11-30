import json
import os


class SeedMetricsLayer:
    def __init__(self, profile, connection=None, database=None, schema=None):
        self.profile_name = profile
        self.metrics_layer, self.connection = self._init_connection(self.profile_name, connection)
        self.database = database
        self.schema = schema

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

    def seed(self):
        import pandas as pd

        from metrics_layer.core.model.definitions import Definitions
        from metrics_layer.core.parse.project_reader import ProjectReader

        if self.connection.type != Definitions.snowflake:
            raise NotImplementedError(
                "The only data warehouse supported for seeding at this time is Snowflake"
            )
        table_query = self.table_query(type="tables")
        table_data = self.run_query(table_query)
        view_query = self.table_query(type="views")
        view_data = self.run_query(view_query)

        columns = ["DATABASE_NAME", "SCHEMA_NAME", "NAME"]
        data = pd.concat([table_data[columns], view_data[columns]], sort=False)

        # Each row represents either a single table or a single view
        views = []
        for _, row in data.iterrows():
            columns_query = self.columns_query(row["NAME"], row["SCHEMA_NAME"], row["DATABASE_NAME"])
            column_data = self.run_query(columns_query)

            column_data["RAW_TYPE"] = column_data["DATA_TYPE"].apply(
                lambda x: json.loads(x).get("type", "TEXT")
            )

            view = self.make_view(column_data, row["NAME"], row["SCHEMA_NAME"])
            print(view)
            views.append(view)

        models = self.make_models(views)

        reader = ProjectReader(None)
        # Fake that the project was loaded from a repo
        reader.unloaded = False

        print(models)
        reader._models = models
        reader._views = views
        # Dump the models to yaml files
        reader.dump(os.getcwd())

    def make_models(self, views: list):
        model = {
            "version": 1,
            "type": "model",
            "name": "base_model",
            "connection": self.connection.name,
            "explores": [{"name": view["name"]} for view in views],
        }
        return [model]

    def make_view(self, column_data, table_name: str, schema_name: str):
        view_name = self.clean_name(table_name)
        count_measure = {"field_type": "measure", "name": "count", "type": "count"}
        fields = self.make_fields(column_data) + [count_measure]
        view = {
            "version": 1,
            "type": "view",
            "name": view_name,
            "sql_table_name": f"{schema_name}.{table_name}",
            "default_date": next((f["name"] for f in fields if f["field_type"] == "dimension_group"), None),
            "row_label": "TODO - Label row",
            "fields": fields,
        }
        return view

    def make_fields(self, column_data: str):
        fields = []
        for _, row in column_data[["COLUMN_NAME", "RAW_TYPE"]].iterrows():
            name = self.clean_name(row["COLUMN_NAME"])
            metrics_layer_type = self._snowflake_type_lookup.get(row["RAW_TYPE"], "string")
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

    def table_query(self, type="tables"):
        query = f"show {type} in "
        if self.database and not self.schema:
            query += f"database {self.database};"
        elif self.database and self.schema:
            query += f"schema {self.database}.{self.schema};"
        else:
            raise ValueError("You must specify a database or a database and a schema for seeding")
        return query

    def columns_query(self, table_name: str, schema_name: str, database_name: str):
        return f'show columns in "{database_name}"."{schema_name}"."{table_name}";'

    def run_query(self, query: str):
        return self.metrics_layer.run_query(query, self.connection)

    @staticmethod
    def _init_connection(profile_name: str, connection_name: str = None):
        metrics_layer = SeedMetricsLayer._init_profile(profile_name)

        if connection_name:
            connection = metrics_layer.config.get_connection(connection_name)
        else:
            connections = metrics_layer.config.connections()
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

        metrics_layer = MetricsLayerConnection(profile_name)
        return metrics_layer

    @staticmethod
    def _init_directories():
        models_dir = "models/"
        views_dir = "views/"
        for directory in [models_dir, views_dir]:
            fully_qualified_path = os.path.join(os.getcwd(), directory)
            if not os.path.exists(fully_qualified_path):
                os.mkdir(fully_qualified_path)

    @staticmethod
    def clean_name(txt: str):
        return txt.lower().replace(" ", "_")
