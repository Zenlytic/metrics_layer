import json
import os

import pandas as pd


class SeedMetricsLayer:
    def __init__(self, profile, connection=None, database=None, schema=None):
        self.profile_name = profile
        self.metrics_layer, self.connection = self._init_connection(self.profile_name, connection)
        self.database = database if database else self.connection.database
        self.schema = schema if schema else self.connection.schema
        if schema:
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

    def seed(self):
        from metrics_layer.core.model.definitions import Definitions
        from metrics_layer.core.parse.project_reader import ProjectReader

        if self.connection.type != Definitions.snowflake:
            raise NotImplementedError(
                "The only data warehouse supported for seeding at this time is Snowflake"
            )
        table_query = self.table_query(type="tables")
        table_data = self.run_query(table_query)
        table_df = self.table_result_to_dataframe(table_data)
        print(f"Got information on all tables in {self.location_description}")

        view_query = self.table_query(type="views")
        view_data = self.run_query(view_query)
        view_df = self.view_result_to_dataframe(view_data)
        print(f"Got information on all views in {self.location_description}")

        data = self.join_view_and_table_results(table_df, view_df)

        # Each row represents either a single table or a single view
        views = []
        for i, (_, row) in enumerate(data.iterrows()):
            columns_query = self.columns_query(row["NAME"], row["SCHEMA_NAME"], row["DATABASE_NAME"])
            column_data = self.run_query(columns_query)
            column_df = self.column_result_to_dataframe(column_data)

            column_df["RAW_TYPE"] = column_df["DATA_TYPE"].apply(lambda x: json.loads(x).get("type", "TEXT"))

            view = self.make_view(column_df, row["NAME"], row["SCHEMA_NAME"])
            print(f"Got information on {row['TYPE']} {row['NAME']} ({i + 1} / {len(data)})")
            views.append(view)

        models = self.make_models(views)

        reader = ProjectReader(None)
        # Fake that the project was loaded from a repo
        reader.unloaded = False

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

    @staticmethod
    def table_result_to_dataframe(table_data):
        table_df = pd.DataFrame(
            [{"NAME": i[1], "DATABASE_NAME": i[2], "SCHEMA_NAME": i[3], "TYPE": "table"} for i in table_data]
        )
        return table_df

    @staticmethod
    def view_result_to_dataframe(view_data):
        view_df = pd.DataFrame(
            [{"NAME": i[1], "DATABASE_NAME": i[3], "SCHEMA_NAME": i[4], "TYPE": "view"} for i in view_data]
        )
        return view_df

    @staticmethod
    def join_view_and_table_results(table_df, view_df):
        columns = ["DATABASE_NAME", "SCHEMA_NAME", "NAME", "TYPE"]
        if view_df.empty and table_df.empty:
            print("Could not find any tables or views, are you sure you are looking ar the right database?")
            raise ValueError("No tables or views found")
        if view_df.empty:
            return table_df[columns]
        if table_df.empty:
            return view_df[columns]
        return pd.concat([table_df[columns], view_df[columns]], sort=False)

    def columns_query(self, table_name: str, schema_name: str, database_name: str):
        return f'show columns in "{database_name}"."{schema_name}"."{table_name}";'

    @staticmethod
    def column_result_to_dataframe(column_data):
        column_df = pd.DataFrame([{"COLUMN_NAME": i[2], "DATA_TYPE": i[3]} for i in column_data])
        return column_df

    def run_query(self, query: str):
        return self.metrics_layer.run_query(query, self.connection, raw_cursor=True)

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
