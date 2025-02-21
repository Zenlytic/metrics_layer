import json
import os
from copy import deepcopy
from typing import Any

from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.query_errors import ArgumentError


class MetricsLayerConnectionError(Exception):
    pass


class ConnectionType:
    snowflake = Definitions.snowflake
    bigquery = Definitions.bigquery
    redshift = Definitions.redshift
    postgres = Definitions.postgres
    druid = Definitions.druid
    sql_server = Definitions.sql_server
    duck_db = Definitions.duck_db
    databricks = Definitions.databricks
    azure_synapse = Definitions.azure_synapse
    trino = Definitions.trino


class BaseConnection:
    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name}>"


class SnowflakeConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        account: str,
        password: str,
        role: str = None,
        username: str = None,
        user: str = None,
        warehouse: str = None,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.snowflake
        self.name = name
        self.account = account
        if user and username:
            raise ArgumentError(
                "Received arguments for both user and username, "
                "please send only one argument for the Snowflake user"
            )
        elif username:
            self.username = username
        elif user:
            self.username = user
        elif kwargs.get("auth_method") != "SSO":
            raise ArgumentError("Received no argument for the Snowflake user, pass either user or username")
        else:
            self.username = username
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def to_dict(self):
        """Dict for use with the snowflake connector"""
        base = {
            "name": self.name,
            "user": self.username,
            "password": self.password,
            "account": self.account,
            "type": self.type,
        }
        if self.warehouse:
            base["warehouse"] = self.warehouse
        if self.database:
            base["database"] = self.database
        if self.schema:
            base["schema"] = self.schema
        if self.role:
            base["role"] = self.role
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "account", "user", "database", "schema", "warehouse", "role"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class DatabricksConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        host: str,
        http_path: str,
        username: str = None,
        password: str = None,
        personal_access_token: str = None,
        client_id: str = None,
        client_secret: str = None,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.databricks
        self.name = name
        self.host = host
        self.http_path = http_path
        self.username = username
        self.password = password
        self.personal_access_token = personal_access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.database = database
        self.schema = schema

    def to_dict(self):
        base = {
            "name": self.name,
            "host": self.host,
            "http_path": self.http_path,
            "username": self.username,
            "password": self.password,
            "personal_access_token": self.personal_access_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "type": self.type,
        }
        if self.database:
            base["database"] = self.database
        if self.schema:
            base["schema"] = self.schema
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "host", "http_path", "username", "database", "schema"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class RedshiftConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        host: str,
        username: str,
        password: str,
        port: int = 5439,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.redshift
        self.name = name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema

    def to_dict(self):
        base = {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "type": self.type,
        }
        if self.database:
            base["database"] = self.database
        if self.schema:
            base["schema"] = self.schema
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "host", "port", "username", "database", "schema"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class PostgresConnection(RedshiftConnection):
    def __init__(
        self,
        name: str,
        host: str,
        user: str,
        password: str,
        port: int = 5432,
        dbname: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.postgres
        self.name = name
        self.host = host
        self.port = port
        self.user = user
        self.username = user
        self.password = password
        self.database = dbname
        self.dbname = dbname
        self.schema = schema


class DuckDBConnection(RedshiftConnection):
    def __init__(
        self,
        name: str,
        host: str,
        user: str,
        password: str,
        port: int = 5432,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.duck_db
        self.name = name
        self.host = host
        self.port = port
        self.user = user
        self.username = user
        self.password = password
        self.database = database
        self.schema = schema


class TrinoConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        host: str,
        user: str,
        database: str,
        schema: str = None,
        scheme: str = "http",
        auth: Any = None,
        port: int = 8080,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.trino
        self.name = name
        self.host = host
        self.user = user
        self.port = port
        self.auth = auth
        self.scheme = scheme
        self.database = database
        self.schema = schema

    def to_dict(self):
        base = {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "scheme": self.scheme,
            "type": self.type,
        }
        if self.user:
            base["user"] = self.user
        if self.auth:
            base["auth"] = self.auth
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "host", "port", "user", "database", "scheme"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class DruidConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        host: str,
        username: str = None,
        user: str = None,
        password: str = None,
        port: int = 8082,
        path: str = "/druid/v2/sql/",
        scheme: str = "http",
        **kwargs,
    ) -> None:
        self.type = ConnectionType.druid
        self.name = name
        self.host = host
        self.port = port
        if user and username:
            raise ArgumentError(
                "Received arguments for both user and username, "
                "please send only one argument for the Druid user"
            )
        elif username:
            self.user = username
        elif user:
            self.user = user
        self.password = password
        self.path = path
        self.scheme = scheme
        self.database = None
        self.schema = None

    def to_dict(self):
        base = {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "path": self.path,
            "scheme": self.scheme,
            "type": self.type,
        }
        if self.user:
            base["user"] = self.user
        if self.password:
            base["password"] = self.password
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "host", "port", "user", "path", "scheme"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


class SQLServerConnection(BaseConnection):
    def __init__(
        self,
        name: str,
        host: str,
        username: str = None,
        user: str = None,
        password: str = None,
        port: int = 1433,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.sql_server
        self.name = name
        self.host = host
        self.port = port
        if user and username:
            raise ArgumentError(
                "Received arguments for both user and username, "
                "please send only one argument for the SQL Server user"
            )
        elif username:
            self.user = username
        elif user:
            self.user = user
        self.password = password
        self.database = database
        self.schema = schema

    def to_dict(self):
        base = {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "type": self.type,
        }
        if self.user:
            base["user"] = self.user
        if self.password:
            base["password"] = self.password
        return base

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("password")
        attributes["name"] = self.name
        sort_order = ["name", "type", "host", "port", "user"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}


# The Synapse connection is the same as SQL Server
class AzureSynapseConnection(SQLServerConnection):
    pass


class BigQueryConnection(BaseConnection):
    def __init__(
        self, name: str, schema: str = None, credentials: str = None, keyfile: str = None, **kwargs
    ) -> None:
        self.type = ConnectionType.bigquery
        self.name = name
        self.schema = schema
        creds_to_use = credentials if credentials else keyfile
        self.credentials = self._convert_json_if_needed(creds_to_use, kwargs)
        self.project_id = self.credentials["project_id"]
        self.database = self.project_id

    def to_dict(self):
        """Dict for use with the BigQuery connector"""
        return {
            "name": self.name,
            "credentials": self.credentials,
            "project_id": self.project_id,
            "type": self.type,
        }

    def printable_attributes(self):
        attributes = deepcopy(self.to_dict())
        attributes.pop("credentials")
        attributes["name"] = self.name
        sort_order = ["name", "type", "project_id"]
        return {key: attributes.get(key) for key in sort_order if attributes.get(key) is not None}

    @staticmethod
    def _convert_json_if_needed(creds: dict, kwargs: dict):
        if isinstance(creds, dict):
            return deepcopy(creds)
        elif isinstance(creds, str):
            try:
                return json.loads(creds)
            except json.JSONDecodeError:
                # This means it's a file path not a JSON string
                if os.path.isabs(creds):
                    path = creds
                else:
                    path = os.path.join(kwargs["directory"], creds)

                with open(path, "r") as f:
                    return json.load(f)
        else:
            raise TypeError(f"BigQuery credentials json had wrong type: {type(creds)} for value {creds}")


connection_class_lookup = {
    ConnectionType.snowflake: SnowflakeConnection,
    ConnectionType.redshift: RedshiftConnection,
    ConnectionType.bigquery: BigQueryConnection,
    ConnectionType.postgres: PostgresConnection,
    ConnectionType.druid: DruidConnection,
    ConnectionType.sql_server: SQLServerConnection,
    ConnectionType.azure_synapse: AzureSynapseConnection,
    ConnectionType.duck_db: DuckDBConnection,
    ConnectionType.databricks: DatabricksConnection,
    ConnectionType.trino: TrinoConnection,
}
