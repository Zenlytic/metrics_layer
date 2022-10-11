import json
import os
from argparse import ArgumentError
from copy import deepcopy

from metrics_layer.core.model.definitions import Definitions


class MetricsLayerConnectionError(Exception):
    pass


class ConnectionType:
    snowflake = Definitions.snowflake
    bigquery = Definitions.bigquery
    redshift = Definitions.redshift
    postgres = Definitions.postgres


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
        else:
            raise ArgumentError("Received no argument for the Snowflake user, pass either user or username")
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
        username: str,
        password: str,
        port: int = 5439,
        database: str = None,
        schema: str = None,
        **kwargs,
    ) -> None:
        self.type = ConnectionType.postgres
        self.name = name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema


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
